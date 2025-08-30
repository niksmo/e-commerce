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

var _ port.ProductsProducer = (*ProductsProducer)(nil)

type ProductsProducer struct {
	cl      ProducerClient
	encoder Encoder
}

func NewProductsProducer(
	opts ...ProducerOpt,
) (ProductsProducer, error) {
	const op = "NewProductsProducer"

	if len(opts) != 2 {
		panic(fmt.Errorf("%s: too few options", op)) // develop mistake
	}

	var options producerOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return ProductsProducer{}, fmt.Errorf("%s: %w", op, err)
		}
	}
	return ProductsProducer{options.cl, options.encoder}, nil
}

func (p ProductsProducer) Close() {
	const op = "ProductsProducer.Close"
	log := slog.With("op", op)
	log.Info("closing producer...")
	p.cl.Close()
	log.Info("producer is closed")
}

func (p ProductsProducer) ProduceProducts(
	ctx context.Context, ps []domain.Product,
) error {
	const op = "ProductsProducer.ProduceProducts"

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

func (p ProductsProducer) createRecords(
	products []domain.Product,
) (rs []*kgo.Record, err error) {
	const op = "ProductsProducer.createRecord"

	for _, product := range products {
		s := p.toSchema(product)
		v, err := p.encoder.Encode(s)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		r := &kgo.Record{Key: []byte(s.Name), Value: v}
		rs = append(rs, r)
	}

	return rs, nil
}

func (p ProductsProducer) produce(
	ctx context.Context, rs []*kgo.Record,
) error {
	const op = "ProductsProducer.produce"
	res := p.cl.ProduceSync(ctx, rs...)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (p ProductsProducer) toSchema(
	product domain.Product,
) (s schema.ProductV1) {
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
