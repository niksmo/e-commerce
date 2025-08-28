package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
	"github.com/niksmo/e-commerce/pkg/schema"
)

var _ port.ProductFilterEmitter = (*ProductFilterEmitter)(nil)

type ProductFilterEmitter struct {
	ge *goka.Emitter
}

func NewProductFilterEmitter() ProductFilterEmitter {
	var (
		brokers []string
		topic   goka.Stream
	)
	ge, err := goka.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		// log.Fatalf("error creating emitter: %v", err)
	}
	return ProductFilterEmitter{ge}
}

func (e ProductFilterEmitter) EmitFilter(
	ctx context.Context, pf domain.ProductFilter,
) error {
	const op = "ProductFilterEmitter.EmitFilter"

	err := e.ge.EmitSync("some-key", "some-value")

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (e ProductFilterEmitter) Close() {
	const op = "ProductFilterEmitter.Close"
	log := slog.With("op", op)

	log.Info("closing emitter...")
	if err := e.ge.Finish(); err != nil {
		log.Error("failed to finish gracefully")
		return
	}
	log.Info("emitter is closed")
}

func (e ProductFilterEmitter) toSchema(
	fv domain.ProductFilter,
) schema.ProductFilterV1 {
	return schema.ProductFilterV1{
		ProductName: fv.ProductName,
		Blocked:     fv.Blocked,
	}
}
