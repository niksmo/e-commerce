package port

import "github.com/niksmo/e-commerce/internal/core/domain"

type ProductsSender interface {
	SendProducts([]domain.Product) error
}

type ProductsProducer interface {
	Produce([]domain.Product) error
}
