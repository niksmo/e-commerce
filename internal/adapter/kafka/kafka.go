package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/lovoo/goka"
	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrTooFewOpts       = errors.New("too few options")
	ErrInvalidValueType = errors.New("invalid value type")
)

type ProducerOpt func(*producerOpts) error

type producerOpts struct {
	cl      ProducerClient
	encoder Encoder
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

type ProducerClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

type ConsumerClient interface {
	PollFetches(context.Context) kgo.Fetches
	CommitUncommittedOffsets(context.Context) error
	Close()
}

type Encoder interface {
	Encode(v any) ([]byte, error)
}

type Decoder interface {
	Decode(b []byte, v any) error
}

type Serde interface {
	Encoder
	Decoder
}

func withNonlogProcOpt() goka.ProcessorOption {
	return goka.WithLogger(log.New(io.Discard, "", 0))
}

func makeOp(s ...string) string {
	return strings.Join(s, ".")
}

func opErr(err error, op ...string) error {
	return fmt.Errorf("%s: %w", makeOp(op...), err)
}

func productToSchemaV1(v domain.Product) (s schema.ProductV1) {
	s.ProductID = v.ProductID
	s.Name = v.Name
	s.SKU = v.SKU
	s.Brand = v.Brand
	s.Category = v.Category
	s.Description = v.Description
	s.Price.Amount = v.Price.Amount
	s.Price.Currency = v.Price.Currency
	s.AvailableStock = v.AvailableStock
	s.Tags = v.Tags
	s.Specifications = v.Specifications
	s.StoreID = v.StoreID

	s.Images = make([]schema.ProductImageV1, len(v.Images))
	for i := range v.Images {
		s.Images[i].URL = v.Images[i].URL
		s.Images[i].Alt = v.Images[i].Alt
	}
	return
}

func productFilterToSchemaV1(
	v domain.ProductFilter,
) (s schema.ProductFilterV1) {
	s.ProductName = v.ProductName
	s.Blocked = v.Blocked
	return
}
