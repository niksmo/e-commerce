package main

import (
	"context"
	"time"

	"github.com/niksmo/e-commerce/config"
	"github.com/niksmo/e-commerce/internal/app"
	"github.com/niksmo/e-commerce/pkg/sigctx"
)

const closeTimeout = 5 * time.Second

func main() {
	sigCtx, closeApp := sigctx.NotifyContext()
	defer closeApp()

	cfg := config.Load()
	cfg.Print()

	service := app.New(sigCtx, cfg)

	service.Run(closeApp)

	<-sigCtx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	service.Close(ctx)
}
