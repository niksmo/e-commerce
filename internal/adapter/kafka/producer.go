package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type ProducerClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

type ProducerClientConfig struct {
	SeedBrokers       []string
	Topic, User, Pass string
	TLSConfig         *tls.Config
}

func NewProducerClient(
	ctx context.Context,
	cfg ProducerClientConfig,
) ProducerClient {
	const op = "ProducerClient"
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.SeedBrokers...),
		kgo.DefaultProduceTopicAlways(),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
		kgo.DialTLSConfig(cfg.TLSConfig),
		kgo.SASL(plain.Auth{User: cfg.User, Pass: cfg.Pass}.AsMechanism()),
	)
	if err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		panic(err)
	}
	if err := cl.Ping(ctx); err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		panic(err)
	}
	return cl
}

type ProducerConfig struct {
	ProducerClient ProducerClient
	Encoder        Encoder
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

func NewProductsProducer(config ProducerConfig) (ProductsProducer, error) {
	opPrefix := "ProductsProducer"
	p := producer{
		opPrefix: opPrefix,
		cl:       config.ProducerClient,
	}

	return ProductsProducer{
		encoder:  config.Encoder,
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
	config ProducerConfig,
) (ProductFilterProducer, error) {
	opPrefix := "ProductFilterProducer"
	p := producer{
		opPrefix: opPrefix,
		cl:       config.ProducerClient,
	}

	return ProductFilterProducer{
		producer: p,
		encoder:  config.Encoder,
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
	config ProducerConfig,
) (FindProductEventProducer, error) {
	opPrefix := "FindProductEventEmitter"
	p := producer{
		opPrefix: opPrefix,
		cl:       config.ProducerClient,
	}

	return FindProductEventProducer{
		producer: p,
		encoder:  config.Encoder,
		opPrefix: opPrefix,
	}, nil
}

func (p FindProductEventProducer) Close() {
	p.producer.close()
}

func (p FindProductEventProducer) Produce(
	ctx context.Context, evt domain.ClientFindProductEvent,
) error {
	const op = "Produce"

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

// A ProductOfferProducer used for produce [domain.ProductOffer]
type ProductOfferProducer struct {
	producer producer
	encoder  Encoder
	opPrefix string
}

func NewProductOfferProducer(
	config ProducerConfig,
) (ProductOfferProducer, error) {
	opPrefix := "ProductOfferProducer"
	p := producer{
		opPrefix: opPrefix,
		cl:       config.ProducerClient,
	}

	return ProductOfferProducer{
		producer: p,
		encoder:  config.Encoder,
		opPrefix: opPrefix,
	}, nil
}

func (p ProductOfferProducer) Close() {
	p.producer.close()
}

func (p ProductOfferProducer) Produce(
	ctx context.Context, v domain.ProductOffer,
) error {
	const op = "Emit"

	if err := ctx.Err(); err != nil {
		return opErr(err, p.opPrefix, op)
	}

	rs, err := p.createRecord(v)
	if err != nil {
		return opErr(err, p.opPrefix, op)
	}

	if err := p.producer.produce(ctx, &rs); err != nil {
		return opErr(err, p.opPrefix, op)
	}

	return nil
}

func (p ProductOfferProducer) createRecord(
	v domain.ProductOffer,
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

func (ProductOfferProducer) toSchema(
	v domain.ProductOffer,
) schema.ProductOfferV1 {
	return productOfferToSchema(v)
}
