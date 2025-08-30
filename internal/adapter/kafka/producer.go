package kafka

import (
	"context"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

type producer struct {
	opPrefix string
	cl       ProducerClient
}

func (p producer) Close() {
	const op = "Close"
	log := slog.With("op", makeOp(p.opPrefix, op))
	log.Info("closing producer...")
	p.cl.Close()
	log.Info("producer is closed")
}

func (p producer) produce(
	ctx context.Context, rs ...*kgo.Record,
) error {
	const op = "produce"
	res := p.cl.ProduceSync(ctx, rs...)
	if err := res.FirstErr(); err != nil {
		return opErr(err, p.opPrefix, op)
	}
	return nil
}

var _ port.ProductsProducer = (*ProductsProducer)(nil)

type ProductsProducer struct {
	producer
	encoder  Encoder
	opPrefix string
}

func NewProductsProducer(
	opts ...ProducerOpt,
) (ProductsProducer, error) {
	const op = "NewProductsProducer"

	if len(opts) != 2 {
		panic(opErr(ErrTooFewOpts, op)) // develop mistake
	}

	var options producerOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return ProductsProducer{}, opErr(err, op)
		}
	}

	opPrefix := "ProductsProducer"
	p := producer{
		opPrefix: opPrefix,
		cl:       options.cl,
	}

	return ProductsProducer{
		encoder:  options.encoder,
		producer: p,
		opPrefix: opPrefix,
	}, nil
}

func (p ProductsProducer) ProduceProducts(
	ctx context.Context, vs []domain.Product,
) error {
	const op = "ProduceProducts"

	if err := ctx.Err(); err != nil {
		return opErr(err, p.opPrefix, op)
	}

	rs, err := p.createRecords(vs)
	if err != nil {
		return opErr(err, p.opPrefix, op)
	}

	if err := p.produce(ctx, rs...); err != nil {
		return opErr(err, p.opPrefix, op)
	}

	return nil
}

func (p ProductsProducer) createRecords(
	vs []domain.Product,
) (rs []*kgo.Record, err error) {
	const op = "createRecord"

	for _, v := range vs {
		s := p.toSchema(v)
		b, err := p.encoder.Encode(s)
		if err != nil {
			return nil, opErr(err, p.opPrefix, op)
		}
		msgKey := []byte(s.Name)
		r := &kgo.Record{Key: msgKey, Value: b}
		rs = append(rs, r)
	}

	return rs, nil
}

func (p ProductsProducer) toSchema(v domain.Product) schema.ProductV1 {
	return productToSchemaV1(v)
}

var _ port.ProductFilterProducer = (*ProductFilterProducer)(nil)

type ProductFilterProducer struct {
	producer
	encoder  Encoder
	opPrefix string
}

func NewProductFilterProducer(
	opts ...ProducerOpt,
) (ProductFilterProducer, error) {
	const op = "NewProductFilterProducer"

	if len(opts) != 2 {
		panic(opErr(ErrTooFewOpts, op)) // develop mistake
	}

	var options producerOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return ProductFilterProducer{}, opErr(err, op)
		}
	}

	opPrefix := "ProductFilterProducer"
	p := producer{
		opPrefix: opPrefix,
		cl:       options.cl,
	}

	return ProductFilterProducer{
		producer: p,
		encoder:  options.encoder,
		opPrefix: opPrefix,
	}, nil
}

func (p ProductFilterProducer) ProduceFilter(
	ctx context.Context, fv domain.ProductFilter,
) error {
	const op = "ProduceFilter"

	if err := ctx.Err(); err != nil {
		return opErr(err, p.opPrefix, op)
	}

	rs, err := p.createRecord(fv)
	if err != nil {
		return opErr(err, p.opPrefix, op)
	}

	if err := p.produce(ctx, &rs); err != nil {
		return opErr(err, p.opPrefix, op)
	}

	return nil
}

func (p ProductFilterProducer) createRecord(
	v domain.ProductFilter,
) (rs kgo.Record, err error) {
	const op = "createRecord"

	s := p.toSchema(v)
	b, err := p.encoder.Encode(s)
	if err != nil {
		return kgo.Record{}, opErr(err, p.opPrefix, op)
	}
	msgKey := []byte(s.ProductName)
	r := kgo.Record{Key: msgKey, Value: b}

	return r, nil
}

func (p ProductFilterProducer) toSchema(
	v domain.ProductFilter,
) schema.ProductFilterV1 {
	return productFilterToSchemaV1(v)
}
