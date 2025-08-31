package kafka

import (
	"context"
	"errors"
	"io"
	"log"
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
///////////////           OPTS            //////////////
////////////////////////////////////////////////////////

type ProcessorOpt func(*processorOpts) error

func WithSeedBrokersProcOpt(seedBrokers ...string) ProcessorOpt {
	return func(po *processorOpts) error {
		if len(seedBrokers) == 0 {
			return errors.New("seed brokers is not set")
		}
		po.seedBrokers = seedBrokers
		return nil
	}
}

func WithGroupProcOpt(
	consumerGroup, inputTopic, outputTopic, joinTopic *string,
) ProcessorOpt {
	return func(po *processorOpts) error {
		po.consumerGroup = (*goka.Group)(consumerGroup)
		po.inputStream = (*goka.Stream)(inputTopic)
		po.outputStream = (*goka.Stream)(outputTopic)
		po.joinTable = (*goka.Table)(joinTopic)
		return nil
	}
}

func WithSerdeProcOpt(s Serde) ProcessorOpt {
	return func(po *processorOpts) error {
		if s == nil {
			return errors.New("serde is not set")
		}
		po.serde = s
		return nil
	}
}

type processorOpts struct {
	seedBrokers   []string
	consumerGroup *goka.Group
	inputStream   *goka.Stream
	joinTable     *goka.Table
	outputStream  *goka.Stream
	serde         Serde
}

func (po *processorOpts) apply(opts ...ProcessorOpt) error {
	for _, opt := range opts {
		if err := opt(po); err != nil {
			return err
		}
	}
	return nil
}

func (po *processorOpts) validate(validations ...func() error) error {
	var errs []error
	for _, validateFn := range validations {
		errs = append(errs, validateFn())
	}
	return errors.Join(errs...)
}

func (po *processorOpts) verifySeedBrokers() error {
	const op = "processorOpts.verifySeedBrokers"
	if len(po.seedBrokers) == 0 {
		return opErr(ErrRequiredOpt, op)
	}
	return nil
}

func (po *processorOpts) verifyConsumerGroup() error {
	const op = "processorOpts.verifyConsumerGroup"
	if po.consumerGroup == nil {
		return opErr(ErrRequiredOpt, op)
	}
	return nil
}

func (po *processorOpts) verifyInputStream() error {
	const op = "processorOpts.verifyInputStream"
	if po.inputStream == nil {
		return opErr(ErrRequiredOpt, op)
	}
	return nil
}

func (po *processorOpts) verifyJoinTable() error {
	const op = "processorOpts.verifyJoinTables"
	if po.joinTable == nil {
		return opErr(ErrRequiredOpt, op)
	}
	return nil
}

func (po *processorOpts) verifyOutputStream() error {
	const op = "processorOpts.verifyOutputStream"
	if po.outputStream == nil {
		return opErr(ErrRequiredOpt, op)
	}
	return nil
}

func (po *processorOpts) verifySerde() error {
	const op = "processorOpts.verifySerde"
	if po.serde == nil {
		return opErr(ErrRequiredOpt, op)
	}
	return nil
}

////////////////////////////////////////////////////////
//////////////           CODECS            /////////////
////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////
//////////////         PROCESSORS          /////////////
////////////////////////////////////////////////////////

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

// A ProductFilterProcessor proccess filter events
// from stream topic to group table.
type ProductFilterProcessor struct {
	opPrefix string
	proc     processor
}

func NewProductFilterProc(
	opts ...ProcessorOpt,
) (*ProductFilterProcessor, error) {
	const op = "NewProductFilterProc"

	var options processorOpts
	if err := options.apply(opts...); err != nil {
		return nil, opErr(err, op)
	}

	err := options.validate(
		options.verifyConsumerGroup,
		options.verifySeedBrokers,
		options.verifyInputStream,
		options.verifySerde,
	)
	if err != nil {
		return nil, opErr(err, op)
	}

	var p ProductFilterProcessor

	gg := goka.DefineGroup(*options.consumerGroup,
		goka.Input(
			*options.inputStream,
			newFilterEventCodec(options.serde),
			p.processFn,
		),
		goka.Persist(blockValueCodec{}),
	)

	gp, err := goka.NewProcessor(options.seedBrokers, gg, withNonlogProcOpt())
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
	opts ...ProcessorOpt,
) (*ProductBlockerProcessor, error) {
	const op = "NewProductBlockerProc"

	var options processorOpts
	if err := options.apply(opts...); err != nil {
		return nil, opErr(err, op)
	}

	err := options.validate(
		options.verifyConsumerGroup,
		options.verifySeedBrokers,
		options.verifyInputStream,
		options.verifyJoinTable,
		options.verifyOutputStream,
		options.verifySerde,
	)
	if err != nil {
		return nil, opErr(err, op)
	}

	var p ProductBlockerProcessor

	productEventCodec := newProductEventCodec(options.serde)

	gg := goka.DefineGroup(*options.consumerGroup,
		goka.Input(*options.inputStream, productEventCodec, p.processFn),
		goka.Join(*options.joinTable, blockValueCodec{}),
		goka.Output(*options.outputStream, productEventCodec),
	)

	gp, err := goka.NewProcessor(options.seedBrokers, gg, withNonlogProcOpt())
	if err != nil {
		return nil, opErr(err, op)
	}

	opPrefix := "ProductBlockerProcessor"
	p.proc = processor{
		opPrefix: opPrefix,
		gp:       gp,
	}
	p.opPrefix = opPrefix
	p.joinedTable = *options.joinTable
	p.outputStream = *options.outputStream
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

func withNonlogProcOpt() goka.ProcessorOption {
	return goka.WithLogger(log.New(io.Discard, "", 0))
}
