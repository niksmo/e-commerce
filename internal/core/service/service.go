package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var _ port.ProductsSender = (*Service)(nil)
var _ port.ProductsFilter = (*Service)(nil)

type Service struct {
	productsProducer       port.ProductsProducer
	productsFilterProducer port.ProductsFilterProducer
}

func New(
	productsProducer port.ProductsProducer,
	productsFilterProducer port.ProductsFilterProducer,
) Service {
	return Service{
		productsProducer,
		productsFilterProducer,
	}
}

func (s Service) SendProducts(ctx context.Context, ps []domain.Product) error {
	const op = "Service.SendProducts"
	log := slog.With("op", op)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	log.Debug("started sending products")

	err := s.productsProducer.Produce(ctx, ps)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	log.Debug("ended sending products")
	return nil
}

func (s Service) SetRule(ctx context.Context, pf domain.ProductFilter) error {
	const op = "Service.SetRule"
	log := slog.With("op", op)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	log.Debug("started setting the rule")

	err := s.productsFilterProducer.Produce(ctx, pf)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	log.Debug("stopped setting the rule")
	return nil
}
