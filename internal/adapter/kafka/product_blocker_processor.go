package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/lovoo/goka"
	"github.com/niksmo/e-commerce/pkg/schema"
)

type productEventCodec struct {
	serde Serde
}

func newProductEventCodec(s Serde) productEventCodec {
	return productEventCodec{s}
}

func (c productEventCodec) Encode(v any) ([]byte, error) {
	const op = "productEventCodec.Encode"
	if _, ok := v.(schema.ProductV1); !ok {
		return nil, fmt.Errorf("%s: invalid value type", op)
	}
	return c.serde.Encode(v)
}

func (c productEventCodec) Decode(data []byte) (any, error) {
	const op = "productEventCodec.Decode"
	var s schema.ProductV1
	err := c.serde.Decode(data, &s)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return s, err
}

type ProductBlockerProcessor struct {
	gp           *goka.Processor
	joinedTable  goka.Table
	outputStream goka.Stream
}

func NewProductBlockerProc(
	seedBrokers []string,
	inputStream string,
	filterGroupTable string,
	outputTopic string,
	productSerde Serde,
) (ProductBlockerProcessor, error) {
	const op = "NewProductFilterProcessor"

	var p ProductBlockerProcessor

	productEventCodec := newProductEventCodec(productSerde)
	intputStream := goka.Stream(inputStream)
	joinedTable := goka.GroupTable(goka.Group(filterGroupTable))
	outputStream := goka.Stream(outputTopic)

	gg := goka.DefineGroup(goka.Group("product-blocker-group"),
		goka.Input(intputStream, productEventCodec, p.processFn),
		goka.Join(joinedTable, blockValueCodec{}),
		goka.Output(outputStream, productEventCodec),
	)

	gp, err := goka.NewProcessor(seedBrokers, gg)
	if err != nil {
		return ProductBlockerProcessor{}, fmt.Errorf("%s: %w", op, err)
	}

	p.gp = gp
	p.joinedTable = joinedTable
	p.outputStream = outputStream
	return p, nil
}

func (p *ProductBlockerProcessor) Run(ctx context.Context) {
	const op = "ProductBlockerProcessor.Run"
	log := slog.With("op", op)

	err := p.gp.Run(ctx)
	if err != nil {
		log.Error("stopped", "err", err)
		return
	}
	log.Info("stopped")
}

func (p *ProductBlockerProcessor) Close() {
	const op = "ProductBlockerProcessor.Close"
	log := slog.With("op", op)

	log.Info("closing processor...")
	p.gp.Stop()
	log.Info("processor is closed")
}

func (p *ProductBlockerProcessor) processFn(ctx goka.Context, msg any) {
	const op = "ProductBlockerProcessor.processFn"

	productV, _ := msg.(schema.ProductV1)
	log := slog.With("op", op, "productName", productV.Name)

	v, ok := ctx.Join(p.joinedTable).(BlockValue)
	if ok && bool(v) {
		log.Warn("product is blocked")
		return
	}
	ctx.Emit(p.outputStream, productV.Name, productV)
	log.Info("product is allowed")
}
