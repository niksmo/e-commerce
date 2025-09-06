package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"log/slog"
	"strconv"
	"sync"

	"github.com/IBM/sarama"
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

func SeedBrokersProcOpt(seedBrokers ...string) ProcessorOpt {
	return func(po *processorOpts) error {
		if len(seedBrokers) == 0 {
			return errors.New("seed brokers is not set")
		}
		po.seedBrokers = seedBrokers
		return nil
	}
}

func GroupProcOpt(consumerGroup *string) ProcessorOpt {
	return func(po *processorOpts) error {
		po.consumerGroup = (*goka.Group)(consumerGroup)
		return nil
	}
}

func InputTopicProcOpt(topic *string) ProcessorOpt {
	return func(po *processorOpts) error {
		po.inputStream = (*goka.Stream)(topic)
		return nil
	}
}
func OutputTopicProcOpt(topic *string) ProcessorOpt {
	return func(po *processorOpts) error {
		po.outputStream = (*goka.Stream)(topic)
		return nil
	}
}
func JoinTopicProcOpt(topic *string) ProcessorOpt {
	return func(po *processorOpts) error {
		po.joinTable = (*goka.Table)(topic)
		return nil
	}
}

func SerdeProcOpt(sEncode, sDecode Serde) ProcessorOpt {
	return func(po *processorOpts) error {
		if sEncode == nil || sDecode == nil {
			return errors.New("not all serdes is set")
		}
		po.serdeEncode = sEncode
		po.serdeDecode = sDecode
		return nil
	}
}

type processorOpts struct {
	seedBrokers   []string
	consumerGroup *goka.Group
	inputStream   *goka.Stream
	joinTable     *goka.Table
	outputStream  *goka.Stream
	serdeEncode   Serde
	serdeDecode   Serde
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

func (po *processorOpts) verifySerdes() error {
	const op = "processorOpts.verifySerdes"
	if po.serdeEncode == nil || po.serdeDecode == nil {
		return opErr(ErrRequiredOpt, op)
	}
	return nil
}

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

	gp, err := goka.NewProcessor(
		config.SeedBrokers,
		gg,
		withTopicManagerBuilder(config.TLSConfig, config.User, config.Pass),
		withNonlogProcOpt(),
	)
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

	gp, err := goka.NewProcessor(
		config.SeedBrokers,
		gg,
		withTopicManagerBuilder(config.TLSConfig, config.User, config.Pass),
		withNonlogProcOpt(),
	)
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

func withNonlogProcOpt() goka.ProcessorOption {
	return goka.WithLogger(log.New(io.Discard, "", 0))
}

func withTopicManagerBuilder(
	tlsConfig *tls.Config, user, pass string,
) goka.ProcessorOption {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = tlsConfig
	saramaCfg.Net.SASL.Enable = true
	saramaCfg.Net.SASL.User = user
	saramaCfg.Net.SASL.Password = pass
	gokaDefaultCfg := goka.NewTopicManagerConfig()
	tmb := goka.TopicManagerBuilderWithConfig(saramaCfg, gokaDefaultCfg)
	return goka.WithTopicManagerBuilder(tmb)
}
