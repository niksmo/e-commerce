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
	"github.com/twmb/franz-go/pkg/sr"
)

var _ port.ProductsProducer = (*ProductsProducer)(nil)

type ProductsProducerOpt func(*productsProducerOpts) error

func ProductsProducerClientOpt(
	ctx context.Context, seedBrokers []string, topic string,
) ProductsProducerOpt {
	return func(opts *productsProducerOpts) error {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(seedBrokers...),
			kgo.DefaultProduceTopicAlways(),
			kgo.DefaultProduceTopic(topic),
		)
		if err != nil {
			return err
		}

		if err := cl.Ping(ctx); err != nil {
			return err
		}
		opts.cl = cl
		return nil
	}
}

func ProductsProducerEncoderOpt(
	ctx context.Context, sc SchemaCreater, subject string,
) ProductsProducerOpt {
	return func(opts *productsProducerOpts) error {
		if sc == nil {
			return errors.New("producer schema creater is nil")
		}
		ss, err := sc.CreateSchema(
			ctx, subject, sr.Schema{
				Type:   sr.TypeAvro,
				Schema: schema.ProductSchemaTextV1,
			},
		)
		if err != nil {
			return err
		}

		serde := new(sr.Serde)
		serde.Register(
			ss.ID,
			schema.ProductV1{},
			sr.EncodeFn(schema.AvroEncodeFn(schema.ProductV1Avro())),
		)
		opts.encoder = serde
		return nil
	}
}

type productsProducerOpts struct {
	cl      ProducerClient
	encoder Encoder
}

type ProductsProducer struct {
	cl      ProducerClient
	encoder Encoder
}

func NewProductsProducer(
	opts ...ProductsProducerOpt,
) (ProductsProducer, error) {
	const op = "NewProductsProducer"

	if len(opts) != 2 {
		panic(fmt.Errorf("%s: too few options", op)) // develop mistake
	}

	var options productsProducerOpts
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
		r := &kgo.Record{Value: v}
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
