package port

import (
	"context"

	"github.com/niksmo/e-commerce/internal/core/domain"
)

type ProductsSender interface {
	SendProducts(context.Context, []domain.Product) error
}

type ProductsFilterSetter interface {
	SetRule(context.Context, domain.ProductFilter) error
}

type ProductsSaver interface {
	SaveProducts(context.Context, []domain.Product) error
}

type ProductsProducer interface {
	ProduceProducts(context.Context, []domain.Product) error
}

type ProductFilterEmitter interface {
	EmitFilter(context.Context, domain.ProductFilter) error
}

type ProductsStorage interface {
	StoreProducts(context.Context, []domain.Product) error
}
