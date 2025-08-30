package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

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
	const op = "filterEventCodec.Encode"
	if _, ok := v.(schema.ProductFilterV1); !ok {
		return nil, fmt.Errorf("%s: invalid value type", op)
	}
	return c.serde.Encode(v)
}

func (c filterEventCodec) Decode(data []byte) (any, error) {
	const op = "filterEventCodec.Decode"
	var s schema.ProductFilterV1
	err := c.serde.Decode(data, &s)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return s, err
}

type BlockValue bool

type blockValueCodec struct{}

func (blockValueCodec) Encode(v any) ([]byte, error) {
	const op = "blockValueCodec.Encode"
	fv, ok := v.(BlockValue)
	if !ok {
		return nil, fmt.Errorf("%s: invalid value type", op)
	}
	data := strconv.AppendBool([]byte(nil), bool(fv))
	return data, nil
}

func (blockValueCodec) Decode(data []byte) (any, error) {
	const op = "blockValueCodec.Decode"
	bv, err := strconv.ParseBool(string(data))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return BlockValue(bv), nil
}

type ProductFilterProcessor struct {
	gp *goka.Processor
}

func NewProductFilterProc(
	seedBrokers []string,
	intputStream string,
	groupTable string,
	productFilterSerde Serde,
) (ProductFilterProcessor, error) {
	const op = "NewProductFilterProcessor"

	var p ProductFilterProcessor

	gg := goka.DefineGroup(goka.Group(groupTable),
		goka.Input(
			goka.Stream(intputStream),
			newFilterEventCodec(productFilterSerde),
			p.processFn,
		),
		goka.Persist(blockValueCodec{}),
	)

	gp, err := goka.NewProcessor(seedBrokers, gg, WithNoLogProcOpt())
	if err != nil {
		return ProductFilterProcessor{}, fmt.Errorf("%s: %w", op, err)
	}

	return ProductFilterProcessor{gp}, nil
}

func (p ProductFilterProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	const op = "ProductFilterProcessor.Run"
	log := slog.With("op", op)

	defer wg.Done()

	go p.run(ctx)

	log.Info("preparing...")
	p.waitForReady(ctx)
	log.Info("running")
}

func (p ProductFilterProcessor) Close() {
	const op = "ProductFilterProcessor.Close"
	log := slog.With("op", op)

	log.Info("closing processor...")
	p.gp.Stop()
	log.Info("processor is closed")
}

func (p ProductFilterProcessor) run(ctx context.Context) {
	const op = "ProductFilterProcessor.run"
	log := slog.With("op", op)

	err := p.gp.Run(ctx)
	if err != nil {
		log.Error("stopped", "err", err)
		return
	}
	log.Info("stopped")
}

func (p ProductFilterProcessor) waitForReady(ctx context.Context) {
	const op = "ProductFilterProcessor.waitForReady"
	log := slog.With("op", op)

	err := p.gp.WaitForReadyContext(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Error("fall down while preparing", "err", err)
		return
	}
}

func (ProductFilterProcessor) processFn(ctx goka.Context, msg any) {
	const op = "ProductFilterProcessor.processFn"
	log := slog.With("op", op)

	event, _ := msg.(schema.ProductFilterV1)
	v := BlockValue(event.Blocked)
	ctx.SetValue(v)
	log.Info("set filter value", "productName", event.ProductName, "isBlocked", v)
}
