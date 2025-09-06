package analytics

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/spark-connect-go/v35/spark/sql"
)

type ClientsDataStorage interface {
	EachData(func(clientData io.ReadCloser) (next bool))
}

type ClientEventsAnalyzer struct {
	s sql.SparkSession
	c chan<- struct{ recommendations []string }
}

func NewClientEventsAnalyzer(
	ctx context.Context, addr string,
) (ClientEventsAnalyzer, error) {
	const op = "NewClientEventsAnalyzer"

	s, err := sql.NewSessionBuilder().Remote(addr).Build(ctx)
	if err != nil {
		return ClientEventsAnalyzer{}, fmt.Errorf("%s: %w", op, err)
	}

	return ClientEventsAnalyzer{s}, nil
}

func (a ClientEventsAnalyzer) DoRecommendations(ctx context.Context) {
}
