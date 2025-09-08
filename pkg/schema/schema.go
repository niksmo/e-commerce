package schema

import (
	"context"

	"github.com/twmb/franz-go/pkg/sr"
)

type SchemaIdentifier interface {
	DetermineID(
		ctx context.Context, subject string, avroSchemaText string,
	) (id int, err error)
}

type lookuperer struct {
	srcl *sr.Client
}

func (i lookuperer) DetermineID(
	ctx context.Context, subject string, avroSchemaText string,
) (id int, err error) {
	ss, err := i.srcl.LookupSchema(ctx, subject, sr.Schema{
		Type:   sr.TypeAvro,
		Schema: avroSchemaText,
	})
	return ss.ID, err
}

type creater struct {
	srcl *sr.Client
}

func (c creater) DetermineID(
	ctx context.Context, subject string, avroSchemaText string,
) (id int, err error) {
	ss, err := c.srcl.CreateSchema(ctx, subject, sr.Schema{
		Type:   sr.TypeAvro,
		Schema: avroSchemaText,
	})
	return ss.ID, err
}

func NewSchemaCreater(srcl *sr.Client) SchemaIdentifier {
	return creater{srcl}
}

func NewSchemaLookuperer(srcl *sr.Client) SchemaIdentifier {
	return lookuperer{srcl}
}

func ValueSubject(topic string) string {
	return topic + "-value"
}
