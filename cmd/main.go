package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/niksmo/e-commerce/config"
	"github.com/niksmo/e-commerce/internal/app"
)

const closeTimeout = 5 * time.Second

var signals = []os.Signal{
	syscall.SIGINT,
	syscall.SIGTERM,
	syscall.SIGQUIT,
}

func main() {
	sigCtx, closeApp := signal.NotifyContext(context.Background(), signals...)
	defer closeApp()

	cfg := config.Load()

	service := app.New(sigCtx, cfg)

	service.Run(closeApp)

	<-sigCtx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	service.Close(ctx)
}
