package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/niksmo/e-commerce/internal/adapter/storage"
	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var (
	ErrNotFound = errors.New("not found")
)

var _ port.ProductsSender = (*Service)(nil)
var _ port.ProductFilterSetter = (*Service)(nil)
var _ port.ProductsSaver = (*Service)(nil)
var _ port.ProductFinder = (*Service)(nil)

type Service struct {
	productFilterProc     port.ProductFilterProcessor
	productBlockerProc    port.ProductBlockerProcessor
	productsProducer      port.ProductsProducer
	productFilterProducer port.ProductFilterProducer
	productsStorage       port.ProductsStorage
	productReader         port.ProductReader
}

func New(
	productFilterProc port.ProductFilterProcessor,
	productBlockerProc port.ProductBlockerProcessor,
	productsProducer port.ProductsProducer,
	productsFilterProducer port.ProductFilterProducer,
	productsStorage port.ProductsStorage,
	productReader port.ProductReader,
) Service {
	return Service{
		productFilterProc, productBlockerProc,
		productsProducer, productsFilterProducer,
		productsStorage, productReader,
	}
}

// Run runs the services components in separate goroutines.
//
// Blocks current goroutine while components is preparing to ready state.
func (s Service) Run(ctx context.Context, stopFn context.CancelFunc) {
	const op = "Service.Run"
	log := slog.With("op", op)

	var wg sync.WaitGroup
	wg.Add(2)
	go s.productFilterProc.Run(ctx, stopFn, &wg)
	go s.productBlockerProc.Run(ctx, stopFn, &wg)
	wg.Wait()

	log.Info("running")
}

func (s Service) Close() {
	const op = "Service.Close"
	log := slog.With("op", op)

	log.Info("service is closing...")
	s.productFilterProc.Close()
	s.productBlockerProc.Close()
	log.Info("service is closed")
}

func (s Service) SendProducts(ctx context.Context, vs []domain.Product) error {
	const op = "Service.SendProducts"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productsProducer.ProduceProducts(ctx, vs)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (s Service) SetRule(ctx context.Context, v domain.ProductFilter) error {
	const op = "Service.SetRule"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productFilterProducer.ProduceFilter(ctx, v)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s Service) SaveProducts(ctx context.Context, vs []domain.Product) error {
	const op = "Service.SaveProducts"
	log := slog.With("op", op)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productsStorage.StoreProducts(ctx, vs)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	var names []string
	for _, v := range vs {
		names = append(names, v.Name)
	}
	n := len(vs)
	log.Info("products saved", "nProducts", n, "products", names)
	return nil
}

func (s Service) FindProduct(
	ctx context.Context, productName string, user string,
) (domain.Product, error) {
	const op = "Service.FindProduct"

	if err := ctx.Err(); err != nil {
		return domain.Product{}, fmt.Errorf("%s: %w", op, err)
	}

	v, err := s.productReader.ReadProduct(ctx, productName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return domain.Product{}, fmt.Errorf("%s: %w", op, ErrNotFound)
		}
		return domain.Product{}, fmt.Errorf("%s: %w", op, err)
	}

	// if user != "" > produce user event

	return v, nil
}
