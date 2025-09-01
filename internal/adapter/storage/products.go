package storage

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var _ port.ProductsStorage = (*ProductsRepository)(nil)

type ProductsRepository struct {
	sql sqlStorage
}

func NewProductsRepository(
	ctx context.Context, dsn string,
) (ProductsRepository, error) {
	const op = "ProductsRepository"

	s, err := newSQLStorage(ctx, dsn)
	if err != nil {
		return ProductsRepository{}, fmt.Errorf("%s: %w", op, err)
	}
	slog.Info("products repository is available", "op", op)
	return ProductsRepository{s}, nil
}

func (r ProductsRepository) StoreProducts(
	ctx context.Context, ps []domain.Product,
) error {
	const op = "ProductsRepository.StoreProducts"
	log := slog.With("op", op)
	log.Info("MOCK upsert successfull")
	return nil
}
