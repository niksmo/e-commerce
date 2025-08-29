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
	ctx        context.Context
	cfg        config.Config
	serdes     serdes
	producers  producers
	service    coreService
	httpServer httphandler.HTTPServer
}

func New(context context.Context, config config.Config) App {
	app := App{ctx: context, cfg: config}

	app.initLogger()
	app.initSerdes()
	app.initOutboundAdapters()
	app.initInboundAdapters()

	return app
}

func (app App) initLogger() {
	opts := &slog.HandlerOptions{Level: app.cfg.LogLevel}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
	slog.SetDefault(logger)
}

func (app App) initSerdes() {
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

func (app App) initOutboundAdapters() {
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

func (app App) initCoreService() {
	s := service.New(
		app.producers.products,
		app.producers.productFilter,
		nil,
	)
	app.service.productsSender = s
	app.service.productFilterSetter = s
	app.service.productsSaver = s
}

func (app App) initInboundAdapters() {
	addr := app.cfg.HTTPServerAddr
	mux := http.NewServeMux()
	httphandler.RegisterProducts(mux, app.service.productsSender)
	httphandler.RegisterFilter(mux, app.service.productFilterSetter)

	handler := httphandler.AllowJSON(mux)
	httpServer := httphandler.NewHTTPServer(addr, handler)
	app.httpServer = httpServer
}

func (app App) Run(stopFn context.CancelFunc) {
	go app.httpServer.Run(stopFn)

	slog.Info("application is running")
}

func (app App) Close(ctx context.Context) {
	slog.Info("application is closing...")

	app.httpServer.Close(ctx)
	app.producers.products.Close()
	app.producers.productFilter.Close()

	slog.Info("application is closed")
}

func (app App) fallDown(op string, err error) {
	panic(fmt.Errorf("%s: %w", op, err))
}
