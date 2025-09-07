package analytics

import (
	"context"
	"log/slog"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/niksmo/e-commerce/internal/core/domain"
)

type ClientEventsAnalyzer struct {
	addr string
}

func NewClientEventsAnalyzer(addr string) ClientEventsAnalyzer {
	return ClientEventsAnalyzer{addr}
}

func (a ClientEventsAnalyzer) Do(
	ctx context.Context, srcPaths []string,
) <-chan domain.Recommendation {
	c := make(chan domain.Recommendation, 1)
	go a.do(ctx, c, srcPaths)
	return c
}

func (a ClientEventsAnalyzer) do(
	ctx context.Context, stream chan<- domain.Recommendation, srcPaths []string,
) {
	const op = "ClientEventsAnalyzer.do"
	log := slog.With("op", op)

	if err := ctx.Err(); err != nil {
		return
	}
	defer close(stream)

	s, err := sql.NewSessionBuilder().Remote(a.addr).Build(ctx)
	if err != nil {
		log.Error("failed to build session", "err", err)
	}

	defer a.stop(s)

	for _, src := range srcPaths {
		df, err := s.Read().Format("json").Load(src)
		if err != nil {
			log.Error("failed to read source")
			return
		}

		nEvents, err := df.Count(ctx)
		if err != nil {
			log.Error("failed to count dataframe rows", "err", err)
			return
		}

		row, err := df.First(ctx)
		if err != nil {
			log.Error("failed to get first row", "err", err)
			return
		}

		username, ok := row.Value("username").(string)
		if !ok {
			log.Error("failed to assert username type: not string")
			return
		}

		stream <- domain.Recommendation{
			Username: username,
			Events:   int(nEvents),
		}
	}

}

func (a ClientEventsAnalyzer) stop(s sql.SparkSession) {
	const op = "ClientEventsAnalyzer.stop"
	log := slog.With("op", op)
	if err := s.Stop(); err != nil {
		log.Error("failed to stop session", "err", err)
	}
}
