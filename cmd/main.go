package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/niksmo/e-commerce/config"
)

func main() {
	sigCtx, cancel := signalContext()
	defer cancel()

	appConfig := config.Load()

	initLogger(appConfig.LogLevel)
	slog.Info("application is running")

	<-sigCtx.Done()
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
