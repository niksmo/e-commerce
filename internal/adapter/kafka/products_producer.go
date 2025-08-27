package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ port.ProductsProducer = (*Producer)(nil)

type ProducerClient interface {
	Close()
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

type ProducerOpt func(*producerOpts) error

func ProducerClientOpt(cl ProducerClient) ProducerOpt {
	return func(opts *producerOpts) error {
		if cl != nil {
			opts.cl = cl
			return nil
		}
		return errors.New("producer client is nil")
	}
}

func ProducerEncodeFnOpt(encodeFn func(v any) ([]byte, error)) ProducerOpt {
	return func(opts *producerOpts) error {
		if encodeFn != nil {
			opts.encodeFn = encodeFn
			return nil
		}
		return errors.New("producer encode func in nil")
	}
}

type producerOpts struct {
	cl       ProducerClient
	encodeFn func(v any) ([]byte, error)
}

type Producer struct {
	cl       ProducerClient
	encodeFn func(v any) ([]byte, error)
}

func NewProducer(opts ...ProducerOpt) Producer {
	const op = "NewProducer"

	if len(opts) == 0 {
		panic(fmt.Errorf("%s: options not set", op)) // develop mistake
	}

	var options producerOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			panic(err) // develop mistake
		}
	}
	return Producer{options.cl, options.encodeFn}
}

func (p Producer) Close() {
	const op = "Producer.Close"
	log := slog.With("op", op)
	log.Info("closing producer...")
	p.cl.Close()
	log.Info("producer is closed")
}

func (p Producer) ProduceProducts(
	ctx context.Context, ps []domain.Product,
) error {
	const op = "Producer.ProduceProducts"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	rs, err := p.createRecords(ps)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if err := p.produce(ctx, rs); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (p Producer) createRecords(
	products []domain.Product,
) (rs []*kgo.Record, err error) {
	const op = "Producer.createRecord"

	for _, product := range products {
		s := p.toSchema(product)
		v, err := p.encodeFn(s)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		r := &kgo.Record{Value: v}
		rs = append(rs, r)
	}

	return rs, nil
}

func (p Producer) produce(ctx context.Context, rs []*kgo.Record) error {
	const op = "Producer.produce"
	res := p.cl.ProduceSync(ctx, rs...)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (p Producer) toSchema(product domain.Product) (s schema.ProductV1) {
	s.ProductID = product.ProductID
	s.Name = product.Name
	s.SKU = product.SKU
	s.Brand = product.Brand
	s.Category = product.Category
	s.Description = product.Description
	s.Price.Amount = product.Price.Amount
	s.Price.Currency = product.Price.Currency
	s.AvailableStock = product.AvailableStock
	s.Tags = product.Tags
	s.Specifications = product.Specifications
	s.StoreID = product.StoreID

	s.Images = make([]schema.ProductImageV1, len(product.Images))
	for i := range product.Images {
		s.Images[i].URL = product.Images[i].URL
		s.Images[i].Alt = product.Images[i].Alt
	}
	return s
}
