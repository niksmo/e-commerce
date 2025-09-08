package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"strconv"
	"sync"

	"github.com/lovoo/goka"
	"github.com/niksmo/e-commerce/pkg/schema"
)

var (
	ErrRequiredOpt      = errors.New("option is required")
	ErrInvalidValueType = errors.New("invalid value type")
)

////////////////////////////////////////////////////////
//////////////           CODECS            /////////////
////////////////////////////////////////////////////////

// A productEventCodec used for serde [schema.ProductV1]
type productEventCodec struct {
	serdeEncode Serde
	serdeDecode Serde
}

func newProductEventCodec(sEncode, sDecode Serde) productEventCodec {
	return productEventCodec{
		serdeEncode: sEncode,
		serdeDecode: sDecode,
	}
}

func (c productEventCodec) Encode(v any) ([]byte, error) {
	const op = "productEventCodec.Encode"
	if _, ok := v.(schema.ProductV1); !ok {
		return nil, opErr(ErrInvalidValueType, op)
	}
	return c.serdeEncode.Encode(v)
}

func (c productEventCodec) Decode(data []byte) (any, error) {
	const op = "productEventCodec.Decode"
	var s schema.ProductV1
	err := c.serdeDecode.Decode(data, &s)
	if err != nil {
		return nil, opErr(err, op)
	}
	return s, err
}

// A filterEventCodec used for serde [schema.ProductFilterV1]
type filterEventCodec struct {
	serdeEncode Serde
	serdeDecode Serde
}

func newFilterEventCodec(sEncode, sDecode Serde) filterEventCodec {
	return filterEventCodec{
		serdeEncode: sEncode,
		serdeDecode: sDecode,
	}
}

func (c filterEventCodec) Encode(v any) ([]byte, error) {
	const op = "filterEventCodec.Encode"
	if _, ok := v.(schema.ProductFilterV1); !ok {
		return nil, opErr(ErrInvalidValueType, op)
	}
	return c.serdeEncode.Encode(v)
}

func (c filterEventCodec) Decode(data []byte) (any, error) {
	const op = "filterEventCodec.Decode"
	var s schema.ProductFilterV1
	err := c.serdeDecode.Decode(data, &s)
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

////////////////////////////////////////////////////////
//////////////         PROCESSORS          /////////////
////////////////////////////////////////////////////////

type processorClient interface {
	Run(ctx context.Context) (rerr error)
	WaitForReadyContext(ctx context.Context) error
	Stop()
}

// A processor is used for composition.
//
// Running and closing the underlying [goka.Processor]
type processor struct {
	opPrefix string
	gp       processorClient
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
	const op = "runProc"
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

// A ProductFilterProcessor used for setup [ProductFilterProcessor].
//
// All fields are required.
type ProductFilterProcessorConfig struct {
	SeedBrokers   []string
	ConsumerGroup string
	InputStream   string
	Encoder       Serde
	Decoder       Serde
	TLSConfig     *tls.Config
	User          string
	Pass          string
}

// A ProductFilterProcessor proccess filter events
// from stream topic to group table.
type ProductFilterProcessor struct {
	opPrefix string
	proc     processor
}

func NewProductFilterProc(
	config ProductFilterProcessorConfig,
) (*ProductFilterProcessor, error) {
	const op = "NewProductFilterProc"

	var p ProductFilterProcessor

	gg := goka.DefineGroup(goka.Group(config.ConsumerGroup),
		goka.Input(
			goka.Stream(config.InputStream),
			newFilterEventCodec(config.Encoder, config.Decoder),
			p.processFn,
		),
		goka.Persist(blockValueCodec{}),
	)

	applySASLTLS(config.TLSConfig, config.User, config.Pass)

	gp, err := goka.NewProcessor(config.SeedBrokers, gg, withNonlogProcOpt())
	if err != nil {
		return nil, opErr(err, op)
	}

	opPrefix := "ProductFilterProcessor"
	p.proc = processor{
		opPrefix: opPrefix,
		gp:       gp,
	}
	p.opPrefix = opPrefix

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

// A ProductBlockerProcessorConfig used for setup [ProductBlockerProcessor].
//
// All fields are required.
type ProductBlockerProcessorConfig struct {
	SeedBrokers   []string
	ConsumerGroup string
	InputStream   string
	JoinTable     string
	OutputStream  string
	Encoder       Serde
	Decoder       Serde
	TLSConfig     *tls.Config
	User          string
	Pass          string
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
	config ProductBlockerProcessorConfig,
) (*ProductBlockerProcessor, error) {
	const op = "NewProductBlockerProc"

	var p ProductBlockerProcessor

	productEventCodec := newProductEventCodec(config.Encoder, config.Decoder)

	gg := goka.DefineGroup(goka.Group(config.ConsumerGroup),
		goka.Input(goka.Stream(config.InputStream), productEventCodec, p.processFn),
		goka.Join(goka.Table(config.JoinTable), blockValueCodec{}),
		goka.Output(goka.Stream(config.OutputStream), productEventCodec),
	)

	applySASLTLS(config.TLSConfig, config.User, config.Pass)

	gp, err := goka.NewProcessor(config.SeedBrokers, gg, withNonlogProcOpt())
	if err != nil {
		return nil, opErr(err, op)
	}

	opPrefix := "ProductBlockerProcessor"
	p.proc = processor{
		opPrefix: opPrefix,
		gp:       gp,
	}
	p.opPrefix = opPrefix
	p.joinedTable = goka.Table(config.JoinTable)
	p.outputStream = goka.Stream(config.OutputStream)
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
