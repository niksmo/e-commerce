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
	"github.com/niksmo/e-commerce/internal/adapter/storage"
	"github.com/niksmo/e-commerce/internal/core/service"
	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/twmb/franz-go/pkg/sr"
)

type serdes struct {
	productFromShop   schema.Serde
	productToStorage  schema.Serde
	productFilter     schema.Serde
	findProductEvents schema.Serde
}

type streamProcessors struct {
	productFilter  *kafka.ProductFilterProcessor
	productBlocker *kafka.ProductBlockerProcessor
}

type storages struct {
	products     storage.ProductsRepository
	clientEvents storage.ClientEventsRepository
}

type producers struct {
	products                 kafka.ProductsProducer
	productFilter            kafka.ProductFilterProducer
	findProductEventProducer kafka.FindProductEventProducer
}

type consumers struct {
	products     kafka.ProductsConsumer
	clientEvents kafka.ClientEventsConsumer
}

type App struct {
	ctx         context.Context
	cfg         config.Config
	serdes      serdes
	streamProcs streamProcessors
	storages    storages
	producers   producers
	consumers   consumers
	coreService *service.Service
	sqldb       storage.SQLDB
	hdfs        storage.HDFS
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
	ctx := app.ctx
	app.coreService.Run(ctx, stopFn)
	go app.consumers.clientEvents.Run(ctx)
	go app.consumers.products.Run(ctx)
	go app.httpServer.Run(stopFn)
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
	app.consumers.products.Close()
	app.consumers.clientEvents.Close()
	app.sqldb.Close()
	app.hdfs.Close()

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
	findProductEventsTopic := app.cfg.Broker.Topics.ClientFindProductEvents

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
	if err != nil {
		app.fallDown(op, err)
	}

	findProductEventsSS := schema.ValueSubject(findProductEventsTopic)
	findProductEventsSerde, err := schema.NewSerdeClientFindProductEventV1(
		ctx,
		schema.SubjectOpt(findProductEventsSS),
		schema.SchemaIdentifierOpt(schemaCreater),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	app.serdes.productFromShop = productFromShopSerde
	app.serdes.productFilter = productFilterSerde
	app.serdes.productToStorage = productToStorageSerde
	app.serdes.findProductEvents = findProductEventsSerde
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
	sqldsn := app.cfg.SQLDB
	hdfsAddr := app.cfg.HDFS.Addr
	hdfsUser := app.cfg.HDFS.User
	seedBrokers := app.cfg.Broker.SeedBrokers
	productsFromShopTopic := app.cfg.Broker.Topics.ProductsFromShop
	filterProductStream := app.cfg.Broker.Topics.FilterProductStream
	findProductEventsTopic := app.cfg.Broker.Topics.ClientFindProductEvents

	sqldb, err := storage.NewSQLDB(ctx, sqldsn)
	if err != nil {
		app.fallDown(op, err)
	}

	hdfs, err := storage.NewHDFS(hdfsAddr, hdfsUser)
	if err != nil {
		app.fallDown(op, err)
	}

	productsRepository := storage.NewProductsRepository(sqldb)
	if err != nil {
		app.fallDown(op, err)
	}

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

	findProductEventProducer, err := kafka.NewFindProductEventProducer(
		kafka.ProducerClientOpt(ctx, seedBrokers, findProductEventsTopic),
		kafka.ProducerEncoderOpt(app.serdes.findProductEvents),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	clientEventsRepository := storage.NewClientEventsRepository(hdfs)

	app.sqldb = sqldb
	app.hdfs = hdfs
	app.storages.products = productsRepository
	app.storages.clientEvents = clientEventsRepository
	app.producers.products = productsProducer
	app.producers.productFilter = productFilterProducer
	app.producers.findProductEventProducer = findProductEventProducer
}

func (app *App) initCoreService() {
	app.coreService = service.New(
		app.streamProcs.productFilter, app.streamProcs.productBlocker,
		app.producers.products, app.producers.productFilter,
		app.storages.products, app.storages.products,
		app.producers.findProductEventProducer,
		app.storages.clientEvents,
	)
}

func (app *App) initInboundAdapters() {
	const op = "App.initInboundAdapters"

	seedBrokers := app.cfg.Broker.SeedBrokers
	productsToStorageTopic := app.cfg.Broker.Topics.ProductsToStorage
	productsSaverGroup := app.cfg.Broker.Consumers.ProductSaverGroup
	findProductEventsTopic := app.cfg.Broker.Topics.ClientFindProductEvents
	clientEventsGroup := app.cfg.Broker.Consumers.ClientEventsGroup

	addr := app.cfg.HTTPServerAddr

	productsConsumer, err := kafka.NewProductsConsumer(
		kafka.ConsumerClientOpt(
			seedBrokers,
			productsToStorageTopic,
			productsSaverGroup,
		),
		kafka.ConsumerDecoderOpt(app.serdes.productToStorage),
		kafka.ProductsConsumerSaverOpt(app.coreService),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	clientEventsConsumer, err := kafka.NewClientEventsConsumer(
		kafka.ConsumerClientOpt(
			seedBrokers,
			findProductEventsTopic,
			clientEventsGroup,
		),
		kafka.ConsumerDecoderOpt(app.serdes.findProductEvents),
		kafka.ClientEventsConsumerSaverOpt(app.coreService),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	handler := http.NewServeMux()
	httphandler.RegisterProducts(handler, app.coreService, app.coreService)
	httphandler.RegisterFilter(handler, app.coreService)
	httpServer := httphandler.NewHTTPServer(addr, handler)

	app.consumers.products = productsConsumer
	app.consumers.clientEvents = clientEventsConsumer
	app.httpServer = httpServer
}

func (app *App) fallDown(op string, err error) {
	panic(fmt.Errorf("%s: %w", op, err))
}
