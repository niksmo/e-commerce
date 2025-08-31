package kafka

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"

	"github.com/lovoo/goka"
	"github.com/niksmo/e-commerce/pkg/schema"
)

// A filterEventCodec used for serde [schema.ProductFilterV1]
type filterEventCodec struct {
	serde Serde
}

func newFilterEventCodec(s Serde) filterEventCodec {
	return filterEventCodec{s}
}

func (c filterEventCodec) Encode(v any) ([]byte, error) {
	const op = "filterEventCodec.Encode"
	if _, ok := v.(schema.ProductFilterV1); !ok {
		return nil, opErr(ErrInvalidValueType, op)
	}
	return c.serde.Encode(v)
}

func (c filterEventCodec) Decode(data []byte) (any, error) {
	const op = "filterEventCodec.Decode"
	var s schema.ProductFilterV1
	err := c.serde.Decode(data, &s)
	if err != nil {
		return nil, opErr(err, op)
	}
	return s, err
}

// A blockValue is represents blocking value for particular product name
type blockValue bool

// A blockValueCodec used for serde [blockValue]
type blockValueCodec struct{}

func (blockValueCodec) Encode(v any) ([]byte, error) {
	const op = "blockValueCodec.Encode"
	fv, ok := v.(blockValue)
	if !ok {
		return nil, opErr(ErrInvalidValueType, op)
	}
	data := strconv.AppendBool([]byte(nil), bool(fv))
	return data, nil
}

func (blockValueCodec) Decode(data []byte) (any, error) {
	const op = "blockValueCodec.Decode"
	bv, err := strconv.ParseBool(string(data))
	if err != nil {
		return nil, opErr(err, op)
	}
	return blockValue(bv), nil
}

// A ProductFilterProcessor proccess filter events
// from stream topic to group table.
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

	gp, err := goka.NewProcessor(seedBrokers, gg, withNoLogProcOpt())
	if err != nil {
		return ProductFilterProcessor{}, opErr(err, op)
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
	v := blockValue(event.Blocked)
	ctx.SetValue(v)
	log.Info(
		"set filter value",
		"productName", event.ProductName,
		"isBlocked", v,
	)
}
