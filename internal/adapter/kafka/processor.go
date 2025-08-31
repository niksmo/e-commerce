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

// A processor is used for composition.
//
// Running and closing the underlying [goka.Processor]
type processor struct {
	opPrefix string
	gp       *goka.Processor
}

func (p *processor) run(
	ctx context.Context, stopFn context.CancelFunc, wg *sync.WaitGroup,
) {
	const op = "run"
	log := slog.With("op", makeOp(p.opPrefix, op))

	defer wg.Done()

	go p.runProc(ctx, stopFn)

	log.Info("preparing...")
	p.waitForReady(ctx)
	log.Info("running")
}

func (p *processor) runProc(ctx context.Context, stopFn context.CancelFunc) {
	const op = "run"
	log := slog.With("op", makeOp(p.opPrefix, op))

	defer stopFn()

	err := p.gp.Run(ctx)
	if err != nil {
		log.Error("stopped", "err", err)
		return
	}
	log.Info("stopped")
}

func (p *processor) waitForReady(ctx context.Context) {
	const op = "waitForReady"
	log := slog.With("op", makeOp(p.opPrefix, op))

	err := p.gp.WaitForReadyContext(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Error("fall down while preparing", "err", err)
		return
	}
}

func (p *processor) close() {
	const op = "close"
	log := slog.With("op", makeOp(p.opPrefix, op))

	log.Info("closing processor...")
	p.gp.Stop()
	log.Info("processor is closed")
}

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
	opPrefix string
	proc     processor
}

func NewProductFilterProc(
	seedBrokers []string,
	intputStream string,
	groupTable string,
	productFilterSerde Serde,
) (*ProductFilterProcessor, error) {
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

	gp, err := goka.NewProcessor(seedBrokers, gg, withNonlogProcOpt())
	if err != nil {
		return nil, opErr(err, op)
	}

	p.proc = processor{
		opPrefix: "ProductFilterProcessor",
		gp:       gp,
	}

	return &p, nil
}

func (p *ProductFilterProcessor) Run(
	ctx context.Context, stopFn context.CancelFunc, wg *sync.WaitGroup,
) {
	p.proc.run(ctx, stopFn, wg)
}

func (p *ProductFilterProcessor) Close() {
	p.proc.close()
}

func (p *ProductFilterProcessor) processFn(ctx goka.Context, msg any) {
	const op = "processFn"
	log := slog.With("op", makeOp(p.opPrefix, op))

	event, _ := msg.(schema.ProductFilterV1)
	v := blockValue(event.Blocked)
	ctx.SetValue(v)
	log.Info(
		"set filter value",
		"productName", event.ProductName,
		"isBlocked", v,
	)
}

// A productEventCodec used for serde [schema.ProductV1]
type productEventCodec struct {
	serde Serde
}

func newProductEventCodec(s Serde) productEventCodec {
	return productEventCodec{s}
}

func (c productEventCodec) Encode(v any) ([]byte, error) {
	const op = "productEventCodec.Encode"
	if _, ok := v.(schema.ProductV1); !ok {
		return nil, opErr(ErrInvalidValueType, op)
	}
	return c.serde.Encode(v)
}

func (c productEventCodec) Decode(data []byte) (any, error) {
	const op = "productEventCodec.Decode"
	var s schema.ProductV1
	err := c.serde.Decode(data, &s)
	if err != nil {
		return nil, opErr(err, op)
	}
	return s, err
}

// A ProductBlockerProcessor proccess products from input stream,
//
// applying filter from group table and send product to output topic.
type ProductBlockerProcessor struct {
	opPrefix     string
	proc         processor
	joinedTable  goka.Table
	outputStream goka.Stream
}

func NewProductBlockerProc(
	seedBrokers []string,
	inputStream string,
	filterGroupTable string,
	outputTopic string,
	productSerde Serde,
) (*ProductBlockerProcessor, error) {
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

	gp, err := goka.NewProcessor(seedBrokers, gg, withNonlogProcOpt())
	if err != nil {
		return nil, opErr(err, op)
	}

	p.proc = processor{
		opPrefix: "ProductBlockerProcessor",
		gp:       gp,
	}
	p.joinedTable = joinedTable
	p.outputStream = outputStream
	return &p, nil
}

func (p *ProductBlockerProcessor) Run(
	ctx context.Context, stopFn context.CancelFunc, wg *sync.WaitGroup,
) {
	p.proc.run(ctx, stopFn, wg)
}

func (p *ProductBlockerProcessor) Close() {
	p.proc.close()
}

func (p *ProductBlockerProcessor) processFn(ctx goka.Context, msg any) {
	const op = "processFn"

	productV, _ := msg.(schema.ProductV1)
	log := slog.With(
		"op", makeOp(p.opPrefix, op), "productName", productV.Name,
	)

	v, ok := ctx.Join(p.joinedTable).(blockValue)
	if ok && bool(v) {
		log.Warn("product is blocked")
		return
	}
	ctx.Emit(p.outputStream, productV.Name, productV)
	log.Info("product is allowed")
}
