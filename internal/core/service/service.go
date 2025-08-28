package service

import (
	"context"
	"fmt"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var _ port.ProductsSender = (*Service)(nil)
var _ port.ProductsFilterSetter = (*Service)(nil)
var _ port.ProductsSaver = (*Service)(nil)

type Service struct {
	productsProducer      port.ProductsProducer
	productFilterProducer port.ProductFilterProducer
	productsStorage       port.ProductsStorage
}

func New(
	productsProducer port.ProductsProducer,
	productsFilterProducer port.ProductFilterProducer,
	productsStorage port.ProductsStorage,
) Service {
	return Service{
		productsProducer,
		productsFilterProducer,
		productsStorage,
	}
}

func (s Service) SendProducts(ctx context.Context, ps []domain.Product) error {
	const op = "Service.SendProducts"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productsProducer.ProduceProducts(ctx, ps)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (s Service) SetRule(ctx context.Context, pf domain.ProductFilter) error {
	const op = "Service.SetRule"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productFilterProducer.ProduceFilter(ctx, pf)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s Service) SaveProducts(ctx context.Context, ps []domain.Product) error {
	const op = "Service.SaveProducts"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productsStorage.StoreProducts(ctx, ps)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}
