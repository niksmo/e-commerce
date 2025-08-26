package service

import (
	"fmt"
	"log/slog"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var _ port.ProductsSender = (*Service)(nil)

type Service struct {
	productsProducer port.ProductsProducer
}

func New() Service {
	return Service{}
}

func (s Service) SendProducts(ps []domain.Product) error {
	const op = "Service.SendProducts"
	log := slog.With("op", op)

	err := s.productsProducer.Produce(ps)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	log.Info("send products")
	return nil
}
