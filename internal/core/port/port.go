package port

import (
	"context"
	"sync"

	"github.com/niksmo/e-commerce/internal/core/domain"
)

type (
	runnerContextWg interface {
		Run(context.Context, context.CancelFunc, *sync.WaitGroup)
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

type ProductFinder interface {
	FindProduct(
		ctx context.Context, productName string, user string,
	) (domain.Product, error)
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

type ProductReader interface {
	ReadProduct(
		ctx context.Context, productName string,
	) (domain.Product, error)
}

type ClientFindProductEventProducer interface {
	Produce(context.Context, domain.ClientFindProductEvent) error
}

type ClientEventsSaver interface {
	SaveEvents(context.Context, []domain.ClientFindProductEvent) error
}

type ClientEventsStorage interface {
	StoreEvents(ctx context.Context,
		username string, evts []domain.ClientFindProductEvent) error
	DataPaths() []string
}

type ClientEventsAnalyzer interface {
	Do(ctx context.Context, srcPaths []string) <-chan domain.ProductOffer
}

type ProductOfferProducer interface {
	Produce(context.Context, domain.ProductOffer) error
}

type ProductOffersGetter interface {
	GetOffers(context.Context) ([]domain.ProductOffer, error)
}

type ProductOffersViewer interface {
	ViewOffers(context.Context) ([]domain.ProductOffer, error)
}
