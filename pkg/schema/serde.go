package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr"
)

var (
	ErrTooFewOpts = errors.New("too few options")
)

type Serde interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

type serde struct {
	avroSchema avro.Schema
	srSerde    *sr.Serde
}

func (s serde) Encode(v any) ([]byte, error) {
	return s.srSerde.Encode(v)
}

func (s serde) Decode(data []byte, v any) error {
	return s.srSerde.Decode(data, v)
}

func (s serde) encodeFn(v any) ([]byte, error) {
	return avro.Marshal(s.avroSchema, v)
}

func (s serde) decodeFn(data []byte, v any) error {
	return avro.Unmarshal(s.avroSchema, data, &v)
}

type Opt func(*serdeOpts) error

type serdeOpts struct {
	subject string
	si      SchemaIdentifier
}

func SubjectOpt(subject string) Opt {
	return func(so *serdeOpts) error {
		if subject == "" {
			return errors.New("subject is empty string")
		}
		so.subject = subject
		return nil
	}
}

func SchemaIdentifierOpt(sc SchemaIdentifier) Opt {
	return func(so *serdeOpts) error {
		if sc == nil {
			return errors.New("schema creater is nil")
		}
		so.si = sc
		return nil
	}
}

func NewSerdeProductV1(ctx context.Context, opts ...Opt) (Serde, error) {
	const op = "NewSerdeProductV1"
	return serdeConstructor(
		ctx,
		ProductSchemaTextV1,
		ProductV1{},
		op,
		opts...,
	)
}

func NewSerdeProducFiltertV1(ctx context.Context, opts ...Opt) (Serde, error) {
	const op = "NewSerdeProductFilterV1"
	return serdeConstructor(
		ctx,
		ProductFilterSchemaTextV1,
		ProductFilterV1{},
		op,
		opts...,
	)
}

func allRequiredOpts(opts []Opt) bool {
	return len(opts) == 2
}

func serdeConstructor(
	ctx context.Context,
	schemaText string,
	example any,
	op string,
	opts ...Opt,
) (Serde, error) {
	if !allRequiredOpts(opts) {
		return serde{}, fmt.Errorf("%s: %w", op, ErrTooFewOpts)
	}

	var serdeOpts serdeOpts
	for _, o := range opts {
		if err := o(&serdeOpts); err != nil {
			return serde{}, fmt.Errorf("%s: %w", op, err)
		}
	}

	avroSchema, err := avro.Parse(schemaText)
	if err != nil {
		return serde{}, fmt.Errorf("%s: %w", op, err)
	}

	s := serde{avroSchema: avroSchema}

	srID, err := serdeOpts.si.DetermineID(
		ctx, serdeOpts.subject, schemaText,
	)
	if err != nil {
		return serde{}, fmt.Errorf("%s: %w", op, err)
	}

	srSerde := new(sr.Serde)
	srSerde.Register(
		srID,
		example,
		sr.EncodeFn(s.encodeFn),
		sr.DecodeFn(s.decodeFn),
	)

	s.srSerde = srSerde
	return s, nil
}
