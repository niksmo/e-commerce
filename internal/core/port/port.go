package port

import (
	"context"
	"sync"

	"github.com/niksmo/e-commerce/internal/core/domain"
)

type (
	runnerContextWg interface {
		Run(context.Context, *sync.WaitGroup)
	}

	closer interface {
		Close()
	}
)

type ProductsSender interface {
	SendProducts(context.Context, []domain.Product) error
}

type ProductFilterSetter interface {
	SetRule(context.Context, domain.ProductFilter) error
}

type ProductsSaver interface {
	SaveProducts(context.Context, []domain.Product) error
}

type ProductsProducer interface {
	ProduceProducts(context.Context, []domain.Product) error
}

type ProductFilterProducer interface {
	ProduceFilter(context.Context, domain.ProductFilter) error
}

type ProductFilterProcessor interface {
	runnerContextWg
	closer
}

type ProductBlockerProcessor interface {
	runnerContextWg
	closer
}

type ProductsStorage interface {
	StoreProducts(context.Context, []domain.Product) error
}
