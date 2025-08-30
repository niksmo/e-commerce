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

type ConsumerOpt func(*productsConsumerOpts) error

type productsConsumerOpts struct {
	cl      ConsumerClient
	decoder Decoder
	saver   port.ProductsSaver
}

type ProductsConsumer struct {
	cl            ConsumerClient
	saver         port.ProductsSaver
	decoder       Decoder
	slowDownTimer *time.Timer
}

func NewProductsConsumer(opts ...ConsumerOpt) ProductsConsumer {
	const op = "NewProductsConsumer"

	if len(opts) != 3 {
		panic(fmt.Errorf("%s: %w", op, ErrTooFewOpts)) // develop mistake
	}

	var options productsConsumerOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			panic(err) // develop mistake
		}
	}

	return ProductsConsumer{
		cl:            options.cl,
		saver:         options.saver,
		decoder:       options.decoder,
		slowDownTimer: time.NewTimer(0),
	}
}

func (c ProductsConsumer) Close() {
	const op = "ProductsConsumer.Close"
	log := slog.With("op", op)

	log.Info("closing consumer...")
	c.slowDownTimer.Stop()
	c.cl.Close()
	log.Info("consumer is closed")
}

func (c ProductsConsumer) Run(ctx context.Context) {
	const op = "ProductsConsumer.Run"
	log := slog.With("op", op)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := c.consume(ctx)
			if err != nil {
				c.handleConsumeErr(err)
				continue
			}

			err = c.commit(ctx)
			if err != nil {
				log.Error("failed to commit offset", "err", err)
			}
		}
	}
}

func (c ProductsConsumer) commit(ctx context.Context) error {
	const op = "ProductsConsumer.commit"

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

func (c ProductsConsumer) consume(ctx context.Context) error {
	const op = "ProductsConsumer.consume"

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	fetches, err := c.pollFetches(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if fetches.Empty() {
		return nil
	}

	ps := c.toProducts(fetches)
	c.saver.SaveProducts(ctx, ps)
	return nil
}

func (c ProductsConsumer) handleConsumeErr(err error) {
	const op = "ProductsConsumer.handleConsumeErr"
	log := slog.With("op", op)

	if errors.Is(err, context.Canceled) {
		log.Info("context cancaled")
		return
	}

	log.Error("failed to consume messages", "err", err)
	c.slowDown()
}

func (c ProductsConsumer) pollFetches(ctx context.Context) (kgo.Fetches, error) {
	const op = "ProductsConsumer.pollFetches"

	fetches := c.cl.PollFetches(ctx)
	if err := fetches.Err0(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	err := c.handleFetchesErrs(fetches)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return fetches, nil
}

func (c ProductsConsumer) handleFetchesErrs(fetches kgo.Fetches) error {
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

func (c ProductsConsumer) toProducts(fetches kgo.Fetches) (ps []domain.Product) {
	const op = "ProductsConsumer.toProducts"
	log := slog.With("op", op)

	fetches.EachRecord(func(r *kgo.Record) {
		var s schema.ProductV1
		err := c.decoder.Decode(r.Value, &s)
		if err != nil {
			err = fmt.Errorf("%s: %w", op, err)
			log.Error("failed to decode value", "err", err)
			return
		}
		p := c.toDomain(s)
		ps = append(ps, p)
	})
	return ps
}

func (c ProductsConsumer) toDomain(s schema.ProductV1) (p domain.Product) {
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

func (c ProductsConsumer) slowDown() {
	const timeout = 1 * time.Second
	c.slowDownTimer.Reset(timeout)
	<-c.slowDownTimer.C
}
