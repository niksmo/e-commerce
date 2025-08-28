package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/niksmo/e-commerce/config"
	"github.com/niksmo/e-commerce/internal/adapter/httphandler"
	"github.com/niksmo/e-commerce/internal/adapter/kafka"
	"github.com/niksmo/e-commerce/internal/core/service"
	"github.com/twmb/franz-go/pkg/sr"
)

func main() {
	sigCtx, closeApp := signalContext()
	defer closeApp()

	cfg := config.Load()

	initLogger(cfg.LogLevel)
	slog.Info("application is running")

	sr := createSRClient(cfg.Broker.SchemaRegistryURLs)

	pp := createProductsProducer(sigCtx, cfg.Broker.SeedBrokers,
		cfg.Broker.ShopProductsTopic, sr)

	service := service.New(pp, nil, nil)

	mux := http.NewServeMux()
	httphandler.RegisterProducts(mux, service)
	httphandler.RegisterFilter(mux, service)

	httpHandler := httphandler.AllowJSON(mux)

	httpServer := createHTTPServer(cfg.HTTPServerAddr, httpHandler)

	go runHTTPServer(httpServer)

	<-sigCtx.Done()
	slog.Info("application is closing...")

	shutdownCtx, cancelTimeout := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancelTimeout()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shutdown http server gracefully")
	}

	pp.Close()

	slog.Info("application is closed")
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT,
	)
}

func initLogger(level slog.Leveler) {
	opts := &slog.HandlerOptions{Level: level}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
	slog.SetDefault(logger)
}

func createHTTPServer(addr string, h http.Handler) *http.Server {
	h = http.TimeoutHandler(h, 5*time.Second, "unavailable")
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Second,
	}
}

func runHTTPServer(s *http.Server) {
	const op = "main.runHTTPServer"

	if err := s.ListenAndServe(); err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		die(op, err)
	}
}

func createSRClient(URLs []string) *sr.Client {
	const op = "main.createSRClient"

	cl, err := sr.NewClient(
		sr.URLs(URLs...),
	)
	if err != nil {
		die(op, err)
	}
	return cl
}

func createProductsProducer(
	ctx context.Context,
	seedBrokers []string,
	topic string,
	sc kafka.SchemaCreater,
) kafka.ProductsProducer {
	const op = "main.craeteProductsProducer"
	p, err := kafka.NewProductsProducer(
		kafka.ProductsProducerClientOpt(ctx, seedBrokers, topic),
		kafka.ProductsProducerEncoderOpt(ctx, sc, topic+"-value"),
	)
	if err != nil {
		die(op, err)
	}
	return p
}

func die(op string, err error) {
	panic(fmt.Errorf("%s: %w", op, err))
}
