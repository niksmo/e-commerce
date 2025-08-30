package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ port.ProductFilterProducer = (*ProductFilterProducer)(nil)

type ProductFilterProducer struct {
	cl      ProducerClient
	encoder Encoder
}

func NewProductFilterProducer(
	opts ...ProducerOpt,
) (ProductFilterProducer, error) {
	const op = "NewProductFilterProducer"

	if len(opts) != 2 {
		panic(fmt.Errorf("%s: %w", op, ErrToFewOpts)) // develop mistake
	}

	var options producerOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return ProductFilterProducer{}, fmt.Errorf("%s: %w", op, err)
		}
	}
	return ProductFilterProducer{options.cl, options.encoder}, nil
}

func (p ProductFilterProducer) Close() {
	const op = "ProductFilterProducer.Close"
	log := slog.With("op", op)
	log.Info("closing producer...")
	p.cl.Close()
	log.Info("producer is closed")
}

func (p ProductFilterProducer) ProduceFilter(
	ctx context.Context, fv domain.ProductFilter,
) error {
	const op = "ProductFilterProducer.ProduceFilter"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	rs, err := p.createRecord(fv)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if err := p.produce(ctx, &rs); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (p ProductFilterProducer) createRecord(
	fv domain.ProductFilter,
) (rs kgo.Record, err error) {
	const op = "ProductFilterProducer.createRecord"

	s := p.toSchema(fv)
	v, err := p.encoder.Encode(s)
	if err != nil {
		return kgo.Record{}, fmt.Errorf("%s: %w", op, err)
	}
	r := kgo.Record{Key: []byte(s.ProductName), Value: v}

	return r, nil
}

func (p ProductFilterProducer) produce(
	ctx context.Context, r *kgo.Record,
) error {
	const op = "ProductFilterProducer.produce"
	res := p.cl.ProduceSync(ctx, r)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (p ProductFilterProducer) toSchema(
	fv domain.ProductFilter,
) (s schema.ProductFilterV1) {
	s.ProductName = fv.ProductName
	s.Blocked = fv.Blocked
	return
}
