package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/lovoo/goka"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/sr"
)

type FilterEventCodec struct {
	serde *sr.Serde
}

func NewFilterEventCodec(
	ctx context.Context, sl SchemaLookuperer, stream string,
) (FilterEventCodec, error) {
	const op = "NewFilterEventCodec"

	ss, err := sl.LookupSchema(ctx, stream+"-value", sr.Schema{
		Type:   sr.TypeAvro,
		Schema: schema.ProductFilterSchemaTextV1,
	})
	if err != nil {
		return FilterEventCodec{}, fmt.Errorf("%s: %w", op, err)
	}

	serde := new(sr.Serde)
	serde.Register(
		ss.ID,
		schema.ProductFilterV1{},
		sr.EncodeFn(schema.AvroEncodeFn(schema.ProductFilterV1Avro())),
		sr.DecodeFn(schema.AvroDecodeFn(schema.ProductFilterV1Avro())),
	)
	return FilterEventCodec{serde}, nil
}

func (c FilterEventCodec) Encode(v any) ([]byte, error) {
	return c.serde.Encode(v)
}

func (c FilterEventCodec) Decode(data []byte) (any, error) {
	var s schema.ProductFilterV1
	err := c.serde.Decode(data, &s)
	return s, err
}

type ProductFilterProcessor struct {
	gp *goka.Processor
}

func NewProductFilterProcessor(
	seedBrokers []string, stream string, group string,
) (ProductFilterProcessor, error) {
	const op = "NewProductFilterProcessor"
	p := ProductFilterProcessor{}

	gg := goka.DefineGroup(goka.Group(group),
		goka.Input(goka.Stream(stream), new(BlockEventCodec), p.processFn),
		goka.Persist(new(BlockValueCodec)),
	)

	var opt goka.ProcessorOption
	gp, err := goka.NewProcessor(seedBrokers, gg, opt)
	if err != nil {
		return ProductFilterProcessor{}, fmt.Errorf("%s: %w", op, err)
	}

	return ProductFilterProcessor{gp}, nil
}

func (p ProductFilterProcessor) Run(ctx context.Context) {
	const op = "ProductFilterProcessor.Run"
	log := slog.With("op", op)

	err := p.gp.Run(ctx)
	if err != nil {
		log.Error("stopped", "err", err)
		return
	}
	log.Info("stopped")
}

func (p ProductFilterProcessor) Close() {
	const op = "ProductFilterProcessor.Close"
	log := slog.With("op", op)

	log.Info("closing processor...")
	p.gp.Stop()
	log.Info("processor is closed")

}

func (ProductFilterProcessor) processFn(ctx goka.Context, msg any) {}
