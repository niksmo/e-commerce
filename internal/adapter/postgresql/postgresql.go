package postgresql

import (
	"context"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var _ port.ProductsStorage = (*ProductsStorage)(nil)

type ProductsStorage struct{}

func New() ProductsStorage {
	return ProductsStorage{}
}

func (s ProductsStorage) StoreProducts(
	ctx context.Context, ps []domain.Product,
) error {
	const op = "ProductsStorage.StoreProducts"
	log := slog.With("op", op)
	log.Info("MOCK upsert successfull")
	return nil
}
