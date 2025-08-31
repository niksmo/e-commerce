package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var _ port.ProductsSender = (*Service)(nil)
var _ port.ProductFilterSetter = (*Service)(nil)
var _ port.ProductsSaver = (*Service)(nil)

type Service struct {
	productsProducer      port.ProductsProducer
	productFilterProducer port.ProductFilterProducer
	productsStorage       port.ProductsStorage
	productFilterProc     port.ProductFilterProcessor
	productBlockerProc    port.ProductBlockerProcessor
}

func New(
	productsProducer port.ProductsProducer,
	productsFilterProducer port.ProductFilterProducer,
	productsStorage port.ProductsStorage,
	productFilterProc port.ProductFilterProcessor,
	productBlockerProc port.ProductBlockerProcessor,
) Service {
	return Service{
		productsProducer,
		productsFilterProducer,
		productsStorage,
		productFilterProc,
		productBlockerProc,
	}
}

// Run runs the services components in separate goroutines.
//
// Blocks current goroutine while components is preparing to ready state.
func (s Service) Run(ctx context.Context, stopFn context.CancelFunc) {
	var wg sync.WaitGroup
	wg.Add(2)
	go s.productFilterProc.Run(ctx, stopFn, &wg)
	go s.productBlockerProc.Run(ctx, stopFn, &wg)
	wg.Wait()
}

func (s Service) Close() {
	s.productFilterProc.Close()
	s.productBlockerProc.Close()
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
