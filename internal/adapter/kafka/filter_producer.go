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

var _ port.ProductFilterProducer = (*ProductFilterProducer)(nil)

type ProductFilterProducerOpt func(*productFilterProducerOpts) error

func ProductFilterProducerClientOpt(
	ctx context.Context, seedBrokers []string, topic string,
) ProductFilterProducerOpt {
	return func(opts *productFilterProducerOpts) error {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(seedBrokers...),
			kgo.DefaultProduceTopicAlways(),
			kgo.DefaultProduceTopic(topic),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.AllowAutoTopicCreation(),
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

func ProductFilterProducerEncoderOpt(
	ctx context.Context, sc SchemaCreater, subject string,
) ProductFilterProducerOpt {
	return func(opts *productFilterProducerOpts) error {
		if sc == nil {
			return errors.New("schema creater is nil")
		}
		ss, err := sc.CreateSchema(
			ctx, subject, sr.Schema{
				Type:   sr.TypeAvro,
				Schema: schema.ProductFilterSchemaTextV1,
			},
		)
		if err != nil {
			return err
		}

		serde := new(sr.Serde)
		serde.Register(
			ss.ID,
			schema.ProductFilterV1{},
			sr.EncodeFn(schema.AvroEncodeFn(schema.ProductFilterV1Avro())),
		)
		opts.encoder = serde
		return nil
	}
}

type productFilterProducerOpts struct {
	cl      ProducerClient
	encoder Encoder
}

type ProductFilterProducer struct {
	cl      ProducerClient
	encoder Encoder
}

func NewProductFilterProducer(
	opts ...ProductFilterProducerOpt,
) (ProductFilterProducer, error) {
	const op = "NewProductFilterProducer"

	if len(opts) != 2 {
		panic(fmt.Errorf("%s: too few options", op)) // develop mistake
	}

	var options productFilterProducerOpts
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
