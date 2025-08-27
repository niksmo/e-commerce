package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

type ProducerClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

type ConsumerClient interface {
	PollFetches(context.Context) kgo.Fetches
	CommitUncommittedOffsets(context.Context) error
	Close()
}

type SchemaCreater interface {
	CreateSchema(
		ctx context.Context, subject string, s sr.Schema,
	) (sr.SubjectSchema, error)
}

type Encoder interface {
	Encode(v any) ([]byte, error)
}

type Decoder interface {
	Decode(b []byte, v any) error
}
