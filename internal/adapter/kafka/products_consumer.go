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

type ConsumerOpt func(*consumerOpts) error

func ConsumerClientOpt(cl ConsumerClient) ConsumerOpt {
	return func(opts *consumerOpts) error {
		if cl != nil {
			opts.cl = cl
			return nil
		}
		return errors.New("consumer client is nil")
	}
}

func ConsumerProductsSaverOpt(s port.ProductsSaver) ConsumerOpt {
	return func(opts *consumerOpts) error {
		if s != nil {
			opts.pSaver = s
			return nil
		}
		return errors.New("consumer products saver is nil")
	}
}

func ConsumerDecodeFnOpt(decodeFn func([]byte, any) error) ConsumerOpt {
	return func(opts *consumerOpts) error {
		if decodeFn != nil {
			opts.decodeFn = decodeFn
			return nil
		}
		return errors.New("consumer decode func is nil")
	}
}

type consumerOpts struct {
	cl       ConsumerClient
	pSaver   port.ProductsSaver
	decodeFn func([]byte, any) error
}

type Consumer struct {
	cl       ConsumerClient
	pSaver   port.ProductsSaver
	decodeFn func([]byte, any) error
	errTimer *time.Timer
}

func NewConsumer(opts ...ConsumerOpt) Consumer {
	const op = "NewConsumer"

	if len(opts) == 0 {
		panic(fmt.Errorf("%s: options not set", op)) // develop mistake
	}

	var options consumerOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			panic(err) // develop mistake
		}
	}

	return Consumer{
		cl:       options.cl,
		pSaver:   options.pSaver,
		decodeFn: options.decodeFn,
		errTimer: time.NewTimer(0),
	}
}

func (c Consumer) Close() {
	const op = "Consumer.Close"
	log := slog.With("op", op)

	log.Info("closing consumer...")
	c.errTimer.Stop()
	c.cl.Close()
	log.Info("consumer is closed")
}

func (c Consumer) Run(ctx context.Context) {
	const op = "Consumer.Run"
	log := slog.With("op", op)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := c.consume(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Info("context cancaled")
					continue
				}
				err = fmt.Errorf("%s: %w", op, err)
				log.Error("failed to consume messages", "err", err)
				c.slowDown()
			}
			err = c.commit(ctx)
			if err != nil {
				log.Error("failed to commit offset", "err", err)
			}
		}
	}
}

func (c Consumer) commit(ctx context.Context) error {
	const op = "Consumer.commit"
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = c.cl.CommitUncommittedOffsets(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (c Consumer) consume(ctx context.Context) error {
	const op = "Consumer.consume"

	fetches, err := c.pollFetches(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if fetches.Empty() {
		return nil
	}

	ps := c.toProducts(fetches)
	c.pSaver.SaveProducts(ctx, ps)
	return nil
}

func (c Consumer) pollFetches(ctx context.Context) (kgo.Fetches, error) {
	const op = "Consumer.pollFetches"

	fetches := c.cl.PollFetches(ctx)
	if err := fetches.Err0(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	err := c.handleErrs(fetches)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return fetches, nil
}

func (c Consumer) handleErrs(fetches kgo.Fetches) error {
	var errsData []string
	fetches.EachError(func(t string, p int32, err error) {
		if err != nil {
			errData := fmt.Sprintf(
				"topic %q partition %d: %q", t, p, err,
			)
			errsData = append(errsData, errData)
		}
	})

	if len(errsData) != 0 {
		return errors.New(strings.Join(errsData, "; "))
	}
	return nil
}

func (c Consumer) toProducts(fetches kgo.Fetches) (ps []domain.Product) {
	const op = "Consumer.toProducts"
	log := slog.With("op", op)

	fetches.EachRecord(func(r *kgo.Record) {
		schema, err := c.unmarshal(r.Value)
		if err != nil {
			err = fmt.Errorf("%s: %w", op, err)
			log.Error("failed to unmarshal value", "err", err)
			return
		}
		p := c.toDomain(schema)
		ps = append(ps, p)
	})
	return ps
}

func (c Consumer) unmarshal(v []byte) (s schema.ProductV1, err error) {
	const op = "Consumer.unmarshal"

	if err := c.decodeFn(v, &s); err != nil {
		return s, fmt.Errorf("%s: %w", op, err)
	}

	return s, nil
}

func (c Consumer) toDomain(s schema.ProductV1) (p domain.Product) {
	p.ProductID = s.ProductID
	p.Name = s.Name
	p.SKU = s.SKU
	p.Brand = s.Brand
	p.Category = s.Category
	p.Description = s.Description
	p.Price.Amount = s.Price.Amount
	p.Price.Currency = s.Price.Currency
	p.AvailableStock = s.AvailableStock
	p.Tags = s.Tags
	p.Specifications = s.Specifications
	p.StoreID = s.StoreID

	p.Images = make([]domain.ProductImage, len(s.Images))
	for i := range s.Images {
		p.Images[i].URL = s.Images[i].URL
		p.Images[i].Alt = s.Images[i].Alt
	}
	return p
}

func (c Consumer) slowDown() {
	const timeout = 1 * time.Second
	c.errTimer.Reset(timeout)
	<-c.errTimer.C
}
