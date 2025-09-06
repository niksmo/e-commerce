package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type ConsumerClient interface {
	PollFetches(context.Context) kgo.Fetches
	CommitUncommittedOffsets(context.Context) error
	Close()
}

////////////////////////////////////////////////////////
////////////           CONSUMERS            ////////////
////////////////////////////////////////////////////////

// A consumer is used for composition.
//
// Fetching records from kafka broker and closing underlying [kgo.Client].

type consumerParent interface {
	processFetches(context.Context, kgo.Fetches) error
}

type consumer struct {
	opPrefix      string
	parent        consumerParent
	cl            ConsumerClient
	slowDownTimer *time.Timer
}

func (c consumer) run(ctx context.Context) {
	const op = "run"
	log := slog.With("op", makeOp(c.opPrefix, op))

	log.Info("running")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := c.consume(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					continue
				}
				log.Error("failed to consume", "err", err)
				c.slowDown()
			}
		}
	}
}

func (c consumer) consume(ctx context.Context) error {
	const op = "consume"

	fetches, err := c.pollFetches(ctx)
	if err != nil {
		return opErr(err, c.opPrefix, op)
	}

	if fetches.Empty() {
		return nil
	}

	err = c.parent.processFetches(ctx, fetches)
	if err != nil {
		return opErr(err, c.opPrefix, op)
	}

	err = c.commit(ctx)
	if err != nil {
		return opErr(err, c.opPrefix, op)
	}
	return nil
}

func (c consumer) pollFetches(ctx context.Context) (kgo.Fetches, error) {
	const op = "pollFetches"

	fetches := c.cl.PollFetches(ctx)
	if err := fetches.Err0(); err != nil {
		return nil, opErr(err, c.opPrefix, op)
	}

	err := c.handleFetchesErrs(fetches)
	if err != nil {
		return nil, opErr(err, c.opPrefix, op)
	}

	return fetches, nil
}

func (c consumer) handleFetchesErrs(fetches kgo.Fetches) error {
	var errsMessages []string
	fetches.EachError(func(t string, p int32, err error) {
		if err != nil {
			errMsg := fmt.Sprintf(
				"topic %q partition %d: %q", t, p, err,
			)
			errsMessages = append(errsMessages, errMsg)
		}
	})

	if len(errsMessages) != 0 {
		return errors.New(strings.Join(errsMessages, "; "))
	}
	return nil
}

func (c consumer) slowDown() {
	c.slowDownTimer.Reset(1 * time.Second)
	<-c.slowDownTimer.C
}

func (c consumer) commit(ctx context.Context) error {
	const op = "commit"

	err := ctx.Err()
	if err != nil {
		return opErr(err, c.opPrefix, op)
	}

	err = c.cl.CommitUncommittedOffsets(ctx)
	if err != nil {
		return opErr(err, c.opPrefix, op)
	}
	return nil
}

func (c consumer) close() {
	const op = "close"
	log := slog.With("op", makeOp(c.opPrefix, op))

	c.slowDownTimer.Stop()

	log.Info("closing consumer...")
	c.cl.Close()
	log.Info("consumer is closed")
}

type ConsumerClientConfig struct {
	SeedBrokers              []string
	Topic, Group, User, Pass string
	TLSConfig                *tls.Config
}

func NewConsumerClient(
	ctx context.Context,
	cfg ConsumerClientConfig,
) ConsumerClient {
	const op = "NewConsumerClient"
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.SeedBrokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.DisableAutoCommit(),
		kgo.DialTLSConfig(cfg.TLSConfig),
		kgo.SASL(plain.Auth{User: cfg.User, Pass: cfg.Pass}.AsMechanism()),
	)
	if err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		panic(err)
	}
	if err := cl.Ping(ctx); err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		panic(err)
	}
	return cl
}

type ProductsConsumerConfig struct {
	ConsumerClient ConsumerClient
	Saver          port.ProductsSaver
	Decoder        Decoder
}

// A ProductsConsumer consumes filtered products
// then sends to the core service for save.
type ProductsConsumer struct {
	opPrefix string
	consumer consumer
	saver    port.ProductsSaver
	decoder  Decoder
}

func NewProductsConsumer(
	config ProductsConsumerConfig,
) (pc ProductsConsumer, err error) {
	const op = "NewProductsConsumer"

	opPrefix := "ProductsConsumer"

	pc.opPrefix = opPrefix
	pc.saver = config.Saver
	pc.decoder = config.Decoder

	pc.consumer = consumer{
		opPrefix:      opPrefix,
		parent:        pc,
		cl:            config.ConsumerClient,
		slowDownTimer: time.NewTimer(0),
	}

	return pc, nil
}

func (c ProductsConsumer) Run(ctx context.Context) {
	c.consumer.run(ctx)
}

func (c ProductsConsumer) Close() {
	c.consumer.close()
}

func (c ProductsConsumer) processFetches(
	ctx context.Context, fetches kgo.Fetches,
) error {
	const op = "processFetches"

	values := c.toDomain(fetches)
	if len(values) == 0 {
		return nil
	}

	err := c.saver.SaveProducts(ctx, values)
	if err != nil {
		return opErr(err, c.opPrefix, op)
	}
	return nil
}

func (c ProductsConsumer) toDomain(
	fetches kgo.Fetches,
) (vs []domain.Product) {
	const op = "toDomain"
	log := slog.With("op", makeOp(c.opPrefix, op))

	fetches.EachRecord(func(r *kgo.Record) {
		v, err := c.decodeRecValue(r)
		if err != nil {
			log.Error(
				"failed to decode value",
				"err", opErr(err, c.opPrefix, op),
			)
			return
		}
		vs = append(vs, v)
	})
	return vs
}

func (c ProductsConsumer) decodeRecValue(
	r *kgo.Record,
) (domain.Product, error) {
	var s schema.ProductV1
	err := c.decoder.Decode(r.Value, &s)
	if err != nil {
		return domain.Product{}, err
	}
	v := schemaV1ToProduct(s)
	return v, nil
}

type ClientEventsConsumerConfig struct {
	ConsumerClient ConsumerClient
	Saver          port.ClientEventsSaver
	Decoder        Decoder
}

// A ClientEventsConsumer consumes clients find product events
// then sends to the core service for save.
type ClientEventsConsumer struct {
	opPrefix string
	consumer consumer
	saver    port.ClientEventsSaver
	decoder  Decoder
}

func NewClientEventsConsumer(
	config ClientEventsConsumerConfig,
) (pc ClientEventsConsumer, err error) {
	const op = "NewClientEventsConsumer"

	opPrefix := "ClientEventsConsumer"

	pc.opPrefix = opPrefix
	pc.saver = config.Saver
	pc.decoder = config.Decoder

	pc.consumer = consumer{
		opPrefix:      opPrefix,
		parent:        pc,
		cl:            config.ConsumerClient,
		slowDownTimer: time.NewTimer(0),
	}

	return pc, nil
}

func (c ClientEventsConsumer) Run(ctx context.Context) {
	c.consumer.run(ctx)
}

func (c ClientEventsConsumer) Close() {
	c.consumer.close()
}

func (c ClientEventsConsumer) processFetches(
	ctx context.Context, fetches kgo.Fetches,
) error {
	const op = "processFetches"

	values := c.toDomain(fetches)
	if len(values) == 0 {
		return nil
	}

	err := c.saver.SaveEvents(ctx, values)
	if err != nil {
		return opErr(err, c.opPrefix, op)
	}
	return nil
}

func (c ClientEventsConsumer) toDomain(
	fetches kgo.Fetches,
) (vs []domain.ClientFindProductEvent) {
	const op = "toDomain"
	log := slog.With("op", makeOp(c.opPrefix, op))

	fetches.EachRecord(func(r *kgo.Record) {
		v, err := c.decodeRecValue(r)
		if err != nil {
			log.Error(
				"failed to decode value",
				"err", opErr(err, c.opPrefix, op),
			)
			return
		}
		vs = append(vs, v)
	})
	return vs
}

func (c ClientEventsConsumer) decodeRecValue(
	r *kgo.Record,
) (domain.ClientFindProductEvent, error) {
	var s schema.ClientFindProductEventV1
	err := c.decoder.Decode(r.Value, &s)
	if err != nil {
		return domain.ClientFindProductEvent{}, err
	}
	v := schemaV1ToClientEvent(s)
	return v, nil
}
