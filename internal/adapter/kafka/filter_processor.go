package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/lovoo/goka"
	"github.com/niksmo/e-commerce/pkg/schema"
)

type filterEventCodec struct {
	serde Serde
}

func newFilterEventCodec(s Serde) filterEventCodec {
	return filterEventCodec{s}
}

func (c filterEventCodec) Encode(v any) ([]byte, error) {
	const op = "FilterEventCodec.Encode"
	if _, ok := v.(schema.ProductFilterV1); !ok {
		return nil, fmt.Errorf("%s: invalid value type", op)
	}
	return c.serde.Encode(v)
}

func (c filterEventCodec) Decode(data []byte) (any, error) {
	const op = "FilterEventCodec.Decode"
	var s schema.ProductFilterV1
	err := c.serde.Decode(data, &s)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return s, err
}

type FilterValue bool

type filterValueCodec struct{}

func (filterValueCodec) Encode(v any) ([]byte, error) {
	const op = "FilterValueCodec.Encode"
	fv, ok := v.(FilterValue)
	if !ok {
		return nil, fmt.Errorf("%s: invalid value type", op)
	}
	data := strconv.AppendBool([]byte(nil), bool(fv))
	return data, nil
}

func (filterValueCodec) Decode(data []byte) (any, error) {
	const op = "FilterValueCodec.Decode"
	bv, err := strconv.ParseBool(string(data))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return FilterValue(bv), nil
}

type ProductFilterProcessor struct {
	gp *goka.Processor
}

func NewProductFilterProcessor(
	seedBrokers []string, stream string, group string, productFilterSerde Serde,
) (ProductFilterProcessor, error) {
	const op = "NewProductFilterProcessor"
	p := ProductFilterProcessor{}

	gg := goka.DefineGroup(goka.Group(group),
		goka.Input(goka.Stream(stream), newFilterEventCodec(productFilterSerde), p.processFn),
		goka.Persist(filterValueCodec{}),
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

func (ProductFilterProcessor) processFn(ctx goka.Context, msg any) {
	event, _ := msg.(schema.ProductFilterV1)
	v := FilterValue(event.Blocked)
	ctx.SetValue(v)
}
