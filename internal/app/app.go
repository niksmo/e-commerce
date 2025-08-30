package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/niksmo/e-commerce/config"
	"github.com/niksmo/e-commerce/internal/adapter/httphandler"
	"github.com/niksmo/e-commerce/internal/adapter/kafka"
	"github.com/niksmo/e-commerce/internal/core/port"
	"github.com/niksmo/e-commerce/internal/core/service"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/sr"
)

type serdes struct {
	product       schema.Serde
	productFilter schema.Serde
}

type streamProcessors struct {
	productFilter kafka.ProductFilterProcessor
}

type producers struct {
	products      kafka.ProductsProducer
	productFilter kafka.ProductFilterProducer
}

type coreService struct {
	productsSender      port.ProductsSender
	productFilterSetter port.ProductFilterSetter
	productsSaver       port.ProductsSaver
}

type App struct {
	ctx              context.Context
	cfg              config.Config
	serdes           serdes
	streamProcessors streamProcessors
	producers        producers
	coreService      service.Service
	httpServer       httphandler.HTTPServer
}

func New(context context.Context, config config.Config) *App {
	app := App{ctx: context, cfg: config}

	app.initLogger()
	app.initSerdes()
	app.initStreamProcessors()
	app.initOutboundAdapters()
	app.initCoreService()
	app.initInboundAdapters()

	return &app
}

func (app *App) Run(stopFn context.CancelFunc) {
	const op = "App.Run"
	log := slog.With("op", op)

	ctx := app.ctx

	go app.httpServer.Run(stopFn)
	go app.streamProcessors.productFilter.Run(ctx)

	log.Info("application is running")
}

func (app *App) Close(ctx context.Context) {
	const op = "App.Close"
	log := slog.With("op", op)
	log.Info("application is closing...")

	app.httpServer.Close(ctx)
	app.producers.products.Close()
	app.producers.productFilter.Close()
	app.streamProcessors.productFilter.Close()

	log.Info("application is closed")
}

func (app *App) initLogger() {
	opts := &slog.HandlerOptions{Level: app.cfg.LogLevel}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
	slog.SetDefault(logger)
}

func (app *App) initSerdes() {
	const op = "App.initSerdes"
	urls := app.cfg.Broker.SchemaRegistryURLs
	ctx := app.ctx

	srClient, err := sr.NewClient(sr.URLs(urls...))
	if err != nil {
		app.fallDown(op, err)
	}

	schemaCreater := schema.NewSchemaCreater(srClient)

	productSS := app.cfg.Broker.ShopProductsTopic + "-value"
	productSerde, err := schema.NewSerdeProductV1(
		ctx,
		schema.SubjectOpt(productSS),
		schema.SchemaIdentifierOpt(schemaCreater),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productFilterSS := app.cfg.Broker.FilterProductStream + "-value"
	productFilterSerde, err := schema.NewSerdeProducFiltertV1(
		ctx,
		schema.SubjectOpt(productFilterSS),
		schema.SchemaIdentifierOpt(schemaCreater),
	)

	app.serdes.product = productSerde
	app.serdes.productFilter = productFilterSerde
}

func (app *App) initStreamProcessors() {
	const op = "App.initStreamProcessors"

	seedBrokers := app.cfg.Broker.SeedBrokers
	filterProductStream := app.cfg.Broker.FilterProductStream
	filterProductGroup := app.cfg.Broker.FilterProductGroup

	productFilterProcessor, err := kafka.NewProductFilterProcessor(
		seedBrokers, filterProductStream, filterProductGroup,
		app.serdes.productFilter,
	)
	if err != nil {
		app.fallDown(op, err)
	}

	app.streamProcessors.productFilter = productFilterProcessor
}

func (app *App) initOutboundAdapters() {
	const op = "App.initOutboundAdapters"

	ctx := app.ctx
	seedBrokers := app.cfg.Broker.SeedBrokers
	shopProductsTopic := app.cfg.Broker.ShopProductsTopic
	productFilterTopic := app.cfg.Broker.FilterProductStream

	productsProducer, err := kafka.NewProductsProducer(
		kafka.ProducerClientOpt(ctx, seedBrokers, shopProductsTopic),
		kafka.ProducerEncoderOpt(app.serdes.product),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productFilterProducer, err := kafka.NewProductFilterProducer(
		kafka.ProducerClientOpt(ctx, seedBrokers, productFilterTopic),
		kafka.ProducerEncoderOpt(app.serdes.productFilter),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	app.producers.products = productsProducer
	app.producers.productFilter = productFilterProducer
}

func (app *App) initCoreService() {
	app.coreService = service.New(
		app.producers.products,
		app.producers.productFilter,
		nil,
	)
}

func (app *App) initInboundAdapters() {
	addr := app.cfg.HTTPServerAddr
	mux := http.NewServeMux()
	httphandler.RegisterProducts(mux, app.coreService)
	httphandler.RegisterFilter(mux, app.coreService)

	handler := httphandler.AllowJSON(mux)
	httpServer := httphandler.NewHTTPServer(addr, handler)
	app.httpServer = httpServer
}

func (app *App) fallDown(op string, err error) {
	panic(fmt.Errorf("%s: %w", op, err))
}
