package kafka

import (
	"context"
	"errors"
	"io"
	"log"

	"github.com/lovoo/goka"
	"github.com/twmb/franz-go/pkg/kgo"
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

type Serde interface {
	Encoder
	Decoder
}

type Encoder interface {
	Encode(v any) ([]byte, error)
}

type Decoder interface {
	Decode(b []byte, v any) error
}

func WithNoLogProcOpt() goka.ProcessorOption {
	return goka.WithLogger(log.New(io.Discard, "", 0))
}
