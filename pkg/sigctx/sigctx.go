package sigctx

import (
	"context"
	"os/signal"
	"syscall"
)

func NotifyContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
}
