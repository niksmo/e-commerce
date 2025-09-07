package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/niksmo/e-commerce/config"
	"github.com/niksmo/e-commerce/internal/adapter"
	"github.com/niksmo/e-commerce/internal/adapter/analytics"
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
	productOffer      schema.Serde
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
	products         kafka.ProductsProducer
	productFilter    kafka.ProductFilterProducer
	findProductEvent kafka.FindProductEventProducer
	productOffer     kafka.ProductOfferProducer
}

type consumers struct {
	products     kafka.ProductsConsumer
	clientEvents kafka.ClientEventsConsumer
}

type analyzers struct {
	clientEvents analytics.ClientEventsAnalyzer
}

type App struct {
	ctx         context.Context
	cfg         config.Config
	serdes      serdes
	streamProcs streamProcessors
	storages    storages
	producers   producers
	consumers   consumers
	analyzers   analyzers
	coreService *service.Service
	sqldb       storage.SQLDB
	hdfs        storage.HDFS
	httpServer  httphandler.HTTPServer
	tlsConfig   *tls.Config
}

func New(context context.Context, config config.Config) *App {
	const op = "App.New"

	app := App{ctx: context, cfg: config}

	app.initLogger()

	log := slog.With("op", op)
	log.Info("initialize application components...")

	app.initTLSConfig()
	app.initSerdes()
	app.initStreamProcessors()
	app.initAnalyzers()
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

func (app *App) initTLSConfig() {
	app.tlsConfig = adapter.MakeTLSConfig(
		app.cfg.Broker.SASLSSL.CACert,
		app.cfg.Broker.SASLSSL.AppCert,
		app.cfg.Broker.SASLSSL.AppKey,
	)
}

func (app *App) initSerdes() {
	const op = "App.initSerdes"
	ctx := app.ctx
	urls := app.cfg.Broker.SchemaRegistryURLs
	productsFromShopTopic := app.cfg.Broker.Topics.ProductsFromShop
	productsToStorageTopic := app.cfg.Broker.Topics.ProductsToStorage
	filterProductsStream := app.cfg.Broker.Topics.FilterProductStream
	findProductEventsTopic := app.cfg.Broker.Topics.ClientFindProductEvents
	productOffersTopic := app.cfg.Broker.Topics.ProductOffers

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

	productOfferSS := schema.ValueSubject(productOffersTopic)
	productOfferSerde, err := schema.NewSerdeProductOfferV1(
		ctx,
		schema.SubjectOpt(productOfferSS),
		schema.SchemaIdentifierOpt(schemaCreater),
	)
	if err != nil {
		app.fallDown(op, err)
	}

	app.serdes.productFromShop = productFromShopSerde
	app.serdes.productFilter = productFilterSerde
	app.serdes.productToStorage = productToStorageSerde
	app.serdes.findProductEvents = findProductEventsSerde
	app.serdes.productOffer = productOfferSerde
}

func (app *App) initAnalyzers() {
	app.analyzers.clientEvents = analytics.NewClientEventsAnalyzer(
		app.cfg.Spark.Addr,
	)
}

func (app *App) initStreamProcessors() {
	const op = "App.initStreamProcessors"

	seedBrokersPrimary := app.cfg.Broker.SeedBrokersPrimary
	filterProductConsumer := app.cfg.Broker.Consumers.FilterProductGroup
	filterProductStream := app.cfg.Broker.Topics.FilterProductStream
	blockerProductConsumer := app.cfg.Broker.Consumers.ProductBlockerGroup
	productsFromShopTopic := app.cfg.Broker.Topics.ProductsFromShop
	filterProductTable := app.cfg.Broker.Topics.FilterProductTable
	productsToStorage := app.cfg.Broker.Topics.ProductsToStorage
	user := app.cfg.Broker.SASLSSL.AppUser
	pass := app.cfg.Broker.SASLSSL.AppPass

	productFilterProc, err := kafka.NewProductFilterProc(
		kafka.ProductFilterProcessorConfig{
			SeedBrokers:   seedBrokersPrimary,
			ConsumerGroup: filterProductConsumer,
			InputStream:   filterProductStream,
			Encoder:       app.serdes.productFilter,
			Decoder:       app.serdes.productFilter,
			TLSConfig:     app.tlsConfig,
			User:          user,
			Pass:          pass,
		},
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productBlockerProc, err := kafka.NewProductBlockerProc(
		kafka.ProductBlockerProcessorConfig{
			SeedBrokers:   seedBrokersPrimary,
			ConsumerGroup: blockerProductConsumer,
			InputStream:   productsFromShopTopic,
			JoinTable:     filterProductTable,
			OutputStream:  productsToStorage,
			Encoder:       app.serdes.productToStorage,
			Decoder:       app.serdes.productFromShop,
			TLSConfig:     app.tlsConfig,
			User:          user,
			Pass:          pass,
		},
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
	seedBrokersPrimary := app.cfg.Broker.SeedBrokersPrimary
	productsFromShopTopic := app.cfg.Broker.Topics.ProductsFromShop
	filterProductStream := app.cfg.Broker.Topics.FilterProductStream
	findProductEventsTopic := app.cfg.Broker.Topics.ClientFindProductEvents
	productOffersTopic := app.cfg.Broker.Topics.ProductOffers
	user := app.cfg.Broker.SASLSSL.AppUser
	pass := app.cfg.Broker.SASLSSL.AppPass

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
		kafka.ProducerConfig{
			ProducerClient: kafka.NewProducerClient(ctx,
				kafka.ProducerClientConfig{
					SeedBrokers: seedBrokersPrimary,
					Topic:       productsFromShopTopic,
					User:        user,
					Pass:        pass,
					TLSConfig:   app.tlsConfig,
				}),
			Encoder: app.serdes.productFromShop,
		},
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productFilterProducer, err := kafka.NewProductFilterProducer(
		kafka.ProducerConfig{
			ProducerClient: kafka.NewProducerClient(ctx,
				kafka.ProducerClientConfig{
					SeedBrokers: seedBrokersPrimary,
					Topic:       filterProductStream,
					User:        user,
					Pass:        pass,
					TLSConfig:   app.tlsConfig,
				}),
			Encoder: app.serdes.productFilter,
		},
	)
	if err != nil {
		app.fallDown(op, err)
	}

	findProductEventProducer, err := kafka.NewFindProductEventProducer(
		kafka.ProducerConfig{
			ProducerClient: kafka.NewProducerClient(ctx,
				kafka.ProducerClientConfig{
					SeedBrokers: seedBrokersPrimary,
					Topic:       findProductEventsTopic,
					User:        user,
					Pass:        pass,
					TLSConfig:   app.tlsConfig,
				}),
			Encoder: app.serdes.findProductEvents,
		},
	)
	if err != nil {
		app.fallDown(op, err)
	}

	productOfferProducer, err := kafka.NewProductOfferProducer(
		kafka.ProducerConfig{
			ProducerClient: kafka.NewProducerClient(ctx,
				kafka.ProducerClientConfig{
					SeedBrokers: seedBrokersPrimary,
					Topic:       productOffersTopic,
					User:        user,
					Pass:        pass,
					TLSConfig:   app.tlsConfig,
				}),
			Encoder: app.serdes.productOffer,
		},
	)
	if err != nil {
		app.fallDown(op, err)
	}

	clientEventsRepository := storage.NewClientEventsRepository(hdfs, app.cfg.HDFS.Host)

	app.sqldb = sqldb
	app.hdfs = hdfs
	app.storages.products = productsRepository
	app.storages.clientEvents = clientEventsRepository
	app.producers.products = productsProducer
	app.producers.productFilter = productFilterProducer
	app.producers.findProductEvent = findProductEventProducer
	app.producers.productOffer = productOfferProducer
}

func (app *App) initCoreService() {
	serviceConfig := service.ServiceConfig{
		ProductFilterProc:        app.streamProcs.productFilter,
		ProductBlockerProc:       app.streamProcs.productBlocker,
		ProductsProducer:         app.producers.products,
		ProductsFilterProducer:   app.producers.productFilter,
		ProductsStorage:          app.storages.products,
		ProductReader:            app.storages.products,
		FindProductEventProducer: app.producers.findProductEvent,
		ClientEventsStorage:      app.storages.clientEvents,
		ClientEventsAnalyzer:     app.analyzers.clientEvents,
		ProductOfferProducer:     app.producers.productOffer,
	}
	app.coreService = service.New(serviceConfig)
}

func (app *App) initInboundAdapters() {
	const op = "App.initInboundAdapters"

	ctx := app.ctx
	seedBrokersPrimary := app.cfg.Broker.SeedBrokersPrimary

	// Because mirror maker is not working
	// seedBrokersSecondary := app.cfg.Broker.SeedBrokersSecondary

	productsToStorageTopic := app.cfg.Broker.Topics.ProductsToStorage
	productsSaverGroup := app.cfg.Broker.Consumers.ProductSaverGroup
	findProductEventsTopic := app.cfg.Broker.Topics.ClientFindProductEvents
	clientEventsGroup := app.cfg.Broker.Consumers.ClientEventsGroup
	addr := app.cfg.HTTPServerAddr
	user := app.cfg.Broker.SASLSSL.AppUser
	pass := app.cfg.Broker.SASLSSL.AppPass

	productsConsumer, err := kafka.NewProductsConsumer(
		kafka.ProductsConsumerConfig{
			ConsumerClient: kafka.NewConsumerClient(ctx,
				kafka.ConsumerClientConfig{
					SeedBrokers: seedBrokersPrimary,
					Topic:       productsToStorageTopic,
					Group:       productsSaverGroup,
					User:        user,
					Pass:        pass,
					TLSConfig:   app.tlsConfig,
				}),
			Decoder: app.serdes.productToStorage,
			Saver:   app.coreService,
		})
	if err != nil {
		app.fallDown(op, err)
	}

	clientEventsConsumer, err := kafka.NewClientEventsConsumer(
		kafka.ClientEventsConsumerConfig{
			ConsumerClient: kafka.NewConsumerClient(ctx,
				kafka.ConsumerClientConfig{
					SeedBrokers: seedBrokersPrimary, // should be "seedBrokersSecondary"
					Topic:       findProductEventsTopic,
					Group:       clientEventsGroup,
					User:        user,
					Pass:        pass,
					TLSConfig:   app.tlsConfig,
				}),
			Decoder: app.serdes.findProductEvents,
			Saver:   app.coreService,
		})
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
