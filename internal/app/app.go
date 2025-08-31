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
	productFromShop  schema.Serde
	productToStorage schema.Serde
	productFilter    schema.Serde
}

type streamProcessors struct {
	productFilter  *kafka.ProductFilterProcessor
	productBlocker *kafka.ProductBlockerProcessor
}

type producers struct {
	products      kafka.ProductsProducer
	productFilter kafka.ProductFilterProducer
}

type App struct {
	ctx         context.Context
	cfg         config.Config
	serdes      serdes
	streamProcs streamProcessors
	producers   producers
	coreService service.Service
	httpServer  httphandler.HTTPServer
}

func New(context context.Context, config config.Config) *App {
	const op = "App.New"

	app := App{ctx: context, cfg: config}

	app.initLogger()

	log := slog.With("op", op)
	log.Info("initialize application components...")

	app.initSerdes()
	app.initStreamProcessors()
	app.initOutboundAdapters()
	app.initCoreService()
	app.initInboundAdapters()

	log.Info("application is ready")

	return &app
}

func (app *App) Run(stopFn context.CancelFunc) {
	const op = "App.Run"
	log := slog.With("op", op)

	ctx := app.ctx

	app.coreService.Run(ctx, stopFn)
	go app.httpServer.Run(stopFn)

	log.Info("application is running")
}

func (app *App) Close(ctx context.Context) {
	const op = "App.Close"
	log := slog.With("op", op)

	log.Info("application is closing...")

	app.httpServer.Close(ctx)
	app.producers.products.Close()
	app.producers.productFilter.Close()
	app.streamProcs.productFilter.Close()
	app.streamProcs.productBlocker.Close()

	log.Info("application is closed")
}

func (app *App) initLogger() {
	opts := &slog.HandlerOptions{Level: app.cfg.LogLevel}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
	slog.SetDefault(logger)
}

func (app *App) initSerdes() {
	const op = "App.initSerdes"
	ctx := app.ctx
	urls := app.cfg.Broker.SchemaRegistryURLs
	productsFromShopTopic := app.cfg.Broker.Topics.ProductsFromShop
	productsToStorageTopic := app.cfg.Broker.Topics.ProductsToStorage
	filterProductsStream := app.cfg.Broker.Topics.FilterProductStream

	srClient, err := sr.NewClient(sr.URLs(urls...))
	if err != nil {
		app.fallDown(op, err)
	}

	schemaCreater := schema.NewSchemaCreater(srClient)

	productFromShopSS := schema.ValueSubject(productsFromShopTopic)
	productFromShopSerde, err := schema.NewSerdeProductV1(
		ctx,
		schema.SubjectOpt(productFromShopSS),
		schema.SchemaIdentifierOpt(schemaCreater),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productFilterSS := schema.ValueSubject(filterProductsStream)
	productFilterSerde, err := schema.NewSerdeProducFiltertV1(
		ctx,
		schema.SubjectOpt(productFilterSS),
		schema.SchemaIdentifierOpt(schemaCreater),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productToStorageSS := schema.ValueSubject(productsToStorageTopic)
	productToStorageSerde, err := schema.NewSerdeProductV1(
		ctx,
		schema.SubjectOpt(productToStorageSS),
		schema.SchemaIdentifierOpt(schemaCreater),
	)

	app.serdes.productFromShop = productFromShopSerde
	app.serdes.productFilter = productFilterSerde
	app.serdes.productToStorage = productToStorageSerde
}

func (app *App) initStreamProcessors() {
	const op = "App.initStreamProcessors"

	seedBrokers := app.cfg.Broker.SeedBrokers
	filterProductConsumer := app.cfg.Broker.Consumers.FilterProductGroup
	filterProductStream := app.cfg.Broker.Topics.FilterProductStream
	blockerProductConsumer := app.cfg.Broker.Consumers.ProductBlockerGroup
	productsFromShopTopic := app.cfg.Broker.Topics.ProductsFromShop
	filterProductTable := app.cfg.Broker.Topics.FilterProductTable
	productsToStorage := app.cfg.Broker.Topics.ProductsToStorage

	productFilterProc, err := kafka.NewProductFilterProc(
		kafka.SeedBrokersProcOpt(seedBrokers...),
		kafka.GroupProcOpt(&filterProductConsumer),
		kafka.InputTopicProcOpt(&filterProductStream),
		kafka.SerdeProcOpt(
			app.serdes.productFilter,
			app.serdes.productFilter,
		),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productBlockerProc, err := kafka.NewProductBlockerProc(
		kafka.SeedBrokersProcOpt(seedBrokers...),
		kafka.GroupProcOpt(&blockerProductConsumer),
		kafka.InputTopicProcOpt(&productsFromShopTopic),
		kafka.JoinTopicProcOpt(&filterProductTable),
		kafka.OutputTopicProcOpt(&productsToStorage),
		kafka.SerdeProcOpt(
			app.serdes.productToStorage,
			app.serdes.productFromShop,
		),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	app.streamProcs.productFilter = productFilterProc
	app.streamProcs.productBlocker = productBlockerProc
}

func (app *App) initOutboundAdapters() {
	const op = "App.initOutboundAdapters"

	ctx := app.ctx
	seedBrokers := app.cfg.Broker.SeedBrokers
	productsFromShopTopic := app.cfg.Broker.Topics.ProductsFromShop
	filterProductStream := app.cfg.Broker.Topics.FilterProductStream

	productsProducer, err := kafka.NewProductsProducer(
		kafka.ProducerClientOpt(ctx, seedBrokers, productsFromShopTopic),
		kafka.ProducerEncoderOpt(app.serdes.productFromShop),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productFilterProducer, err := kafka.NewProductFilterProducer(
		kafka.ProducerClientOpt(ctx, seedBrokers, filterProductStream),
		kafka.ProducerEncoderOpt(app.serdes.productFilter),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	app.producers.products = productsProducer
	app.producers.productFilter = productFilterProducer
}

func (app *App) initCoreService() {
	var nilProductsStorage port.ProductsStorage

	app.coreService = service.New(
		app.producers.products,
		app.producers.productFilter,
		nilProductsStorage,
		app.streamProcs.productFilter,
		app.streamProcs.productBlocker,
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
