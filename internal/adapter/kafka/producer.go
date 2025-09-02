package kafka

import (
	"context"
	"errors"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ProducerClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

////////////////////////////////////////////////////////
///////////////           OPTS            //////////////
////////////////////////////////////////////////////////

type ProducerOpt func(*producerOpts) error

type producerOpts struct {
	cl      ProducerClient
	encoder Encoder
}

func (po *producerOpts) apply(opts ...ProducerOpt) error {
	if len(opts) != 2 {
		return ErrTooFewOpts
	}

	for _, opt := range opts {
		if err := opt(po); err != nil {
			return err
		}
	}

	return nil
}

func ProducerClientOpt(
	ctx context.Context, seedBrokers []string, topic string,
) ProducerOpt {
	return func(opts *producerOpts) error {
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

func ProducerEncoderOpt(encoder Encoder) ProducerOpt {
	return func(opts *producerOpts) error {
		if encoder == nil {
			return errors.New("encoder is nil")
		}
		opts.encoder = encoder
		return nil
	}
}

////////////////////////////////////////////////////////
//////////////         PRODUCERS          //////////////
////////////////////////////////////////////////////////

// A producer is used for composition.
//
// Producing records to kafka broker and closing underlying [kgo.Client].
type producer struct {
	opPrefix string
	cl       ProducerClient
}

func (p producer) close() {
	const op = "close"
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

// A ProductsProducer used for produce [domain.Product]
type ProductsProducer struct {
	producer producer
	encoder  Encoder
	opPrefix string
}

func NewProductsProducer(
	opts ...ProducerOpt,
) (ProductsProducer, error) {
	const op = "NewProductsProducer"

	var options producerOpts
	if err := options.apply(opts...); err != nil {
		return ProductsProducer{}, opErr(err, op)
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

func (p ProductsProducer) Close() {
	p.producer.close()
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

	if err := p.producer.produce(ctx, rs...); err != nil {
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

func (ProductsProducer) toSchema(v domain.Product) schema.ProductV1 {
	return productToSchemaV1(v)
}

// A ProductFilterProducer used for produce [domain.ProductFilter]
type ProductFilterProducer struct {
	producer producer
	encoder  Encoder
	opPrefix string
}

func NewProductFilterProducer(
	opts ...ProducerOpt,
) (ProductFilterProducer, error) {
	const op = "NewProductFilterProducer"

	var options producerOpts
	if err := options.apply(opts...); err != nil {
		return ProductFilterProducer{}, opErr(err, op)
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

func (p ProductFilterProducer) Close() {
	p.producer.close()
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

	if err := p.producer.produce(ctx, &rs); err != nil {
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

func (ProductFilterProducer) toSchema(
	v domain.ProductFilter,
) schema.ProductFilterV1 {
	return productFilterToSchemaV1(v)
}

// A FindProductEventProducer used for produce [domain.ClientFindProductEvent]
type FindProductEventProducer struct {
	producer producer
	encoder  Encoder
	opPrefix string
}

func NewFindProductEventProducer(
	opts ...ProducerOpt,
) (FindProductEventProducer, error) {
	const op = "FindProductEventEmitter"

	var options producerOpts
	if err := options.apply(opts...); err != nil {
		return FindProductEventProducer{}, opErr(err, op)
	}

	opPrefix := "FindProductEventEmitter"
	p := producer{
		opPrefix: opPrefix,
		cl:       options.cl,
	}

	return FindProductEventProducer{
		producer: p,
		encoder:  options.encoder,
		opPrefix: opPrefix,
	}, nil
}

func (p FindProductEventProducer) Close() {
	p.producer.close()
}

func (p FindProductEventProducer) Emit(
	ctx context.Context, evt domain.ClientFindProductEvent,
) error {
	const op = "Emit"

	if err := ctx.Err(); err != nil {
		return opErr(err, p.opPrefix, op)
	}

	rs, err := p.createRecord(evt)
	if err != nil {
		return opErr(err, p.opPrefix, op)
	}

	if err := p.producer.produce(ctx, &rs); err != nil {
		return opErr(err, p.opPrefix, op)
	}

	return nil
}

func (p FindProductEventProducer) createRecord(
	v domain.ClientFindProductEvent,
) (rs kgo.Record, err error) {
	const op = "createRecord"

	s := p.toSchema(v)
	b, err := p.encoder.Encode(s)
	if err != nil {
		return kgo.Record{}, opErr(err, p.opPrefix, op)
	}
	msgKey := []byte(s.Username)
	r := kgo.Record{Key: msgKey, Value: b}

	return r, nil
}

func (FindProductEventProducer) toSchema(
	v domain.ClientFindProductEvent,
) schema.ClientFindProductEventV1 {
	return clientEventToSchema(v)
}
