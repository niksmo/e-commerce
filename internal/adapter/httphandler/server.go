package httphandler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

type HTTPServer struct {
	httpServer *http.Server
}

func NewHTTPServer(addr string, handler http.Handler) HTTPServer {
	handler = http.TimeoutHandler(handler, 5*time.Second, "unavailable")
	s := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Second,
	}
	return HTTPServer{s}
}

func (s HTTPServer) Run(stopFn context.CancelFunc) {
	const op = "HTTPServer.Run"
	log := slog.With("op", op)

	defer stopFn()
	err := s.httpServer.ListenAndServe()
	if err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		log.Error("unexpected servers shutdown", "err", err)
	}
}

func (s HTTPServer) Close(ctx context.Context) {
	const op = "HTTPServer.Close"
	log := slog.With("op", op)

	log.Info("closing http server...")

	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		log.Error("failed to shutdown gracefully", "err", err)
	}
	log.Info("http server is closed")
}
