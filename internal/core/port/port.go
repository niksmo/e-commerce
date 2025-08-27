package port

import (
	"context"

	"github.com/niksmo/e-commerce/internal/core/domain"
)

type ProductsSender interface {
	SendProducts(context.Context, []domain.Product) error
}

type ProductsFilter interface {
	SetRule(context.Context, domain.ProductFilter) error
}

type ProductsProducer interface {
	Produce(context.Context, []domain.Product) error
}

type ProductsFilterProducer interface {
	Produce(context.Context, domain.ProductFilter) error
}
