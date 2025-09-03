package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumerClient interface {
	PollFetches(context.Context) kgo.Fetches
	CommitUncommittedOffsets(context.Context) error
	Close()
}

////////////////////////////////////////////////////////
///////////////           OPTS            //////////////
////////////////////////////////////////////////////////

type ConsumerOpt func(*consumerOpts) error

func ConsumerClientOpt(
	seedBrokers []string, topic, group string,
) ConsumerOpt {
	return func(co *consumerOpts) error {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(seedBrokers...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.DisableAutoCommit(),
		)
		if err != nil {
			return err
		}
		co.cl = cl
		return nil
	}
}

func ConsumerDecoderOpt(decoder Decoder) ConsumerOpt {
	return func(co *consumerOpts) error {
		if decoder == nil {
			return errors.New("decoder is nil")
		}
		co.decoder = decoder
		return nil
	}
}

func ProductsConsumerSaverOpt(ps port.ProductsSaver) ConsumerOpt {
	return func(co *consumerOpts) error {
		if ps == nil {
			return errors.New("products saver is nil")
		}
		co.productsSaver = ps
		return nil
	}
}

type consumerOpts struct {
	cl                ConsumerClient
	decoder           Decoder
	productsSaver     port.ProductsSaver
	clientEventsSaver port.ClientEventsSaver
}

func (co *consumerOpts) apply(opts ...ConsumerOpt) error {
	for _, opt := range opts {
		if err := opt(co); err != nil {
			return err
		}
	}
	return nil
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

// A ProductsConsumer consumes filtered products
// then sends to the core service for save.
type ProductsConsumer struct {
	opPrefix string
	consumer consumer
	saver    port.ProductsSaver
	decoder  Decoder
}

func NewProductsConsumer(opts ...ConsumerOpt) (pc ProductsConsumer, err error) {
	const op = "NewProductsConsumer"

	var options consumerOpts
	if err := options.apply(opts...); err != nil {
		return pc, opErr(err, op)
	}

	opPrefix := "ProductsConsumer"

	pc.opPrefix = opPrefix
	pc.saver = options.productsSaver
	pc.decoder = options.decoder

	pc.consumer = consumer{
		opPrefix:      opPrefix,
		parent:        pc,
		cl:            options.cl,
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

// A ClientEventsConsumer consumes clients find product events
// then sends to the core service for save.
type ClientEventsConsumer struct {
	opPrefix string
	consumer consumer
	saver    port.ClientEventsSaver
	decoder  Decoder
}

func NewClientEventsConsumer(opts ...ConsumerOpt) (pc ClientEventsConsumer, err error) {
	const op = "NewClientEventsConsumer"

	var options consumerOpts
	if err := options.apply(opts...); err != nil {
		return pc, opErr(err, op)
	}

	opPrefix := "ClientEventsConsumer"

	pc.opPrefix = opPrefix
	pc.saver = options.clientEventsSaver
	pc.decoder = options.decoder

	pc.consumer = consumer{
		opPrefix:      opPrefix,
		parent:        pc,
		cl:            options.cl,
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
