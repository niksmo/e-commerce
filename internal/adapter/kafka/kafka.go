package kafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/pkg/schema"
)

var (
	ErrTooFewOpts = errors.New("too few options")
)

type (
	Serde interface {
		Encoder
		Decoder
	}

	Encoder interface {
		Encode(v any) ([]byte, error)
	}

	Decoder interface {
		Decode(b []byte, v any) error
	}
)

func makeOp(s ...string) string {
	return strings.Join(s, ".")
}

func opErr(err error, op ...string) error {
	return fmt.Errorf("%s: %w", makeOp(op...), err)
}

func productToSchemaV1(v domain.Product) (s schema.ProductV1) {
	s.ProductID = v.ProductID
	s.Name = v.Name
	s.SKU = v.SKU
	s.Brand = v.Brand
	s.Category = v.Category
	s.Description = v.Description
	s.Price.Amount = v.Price.Amount
	s.Price.Currency = v.Price.Currency
	s.AvailableStock = v.AvailableStock
	s.Tags = v.Tags
	s.Specifications = v.Specifications
	s.StoreID = v.StoreID

	s.Images = make([]schema.ProductImageV1, len(v.Images))
	for i := range v.Images {
		s.Images[i].URL = v.Images[i].URL
		s.Images[i].Alt = v.Images[i].Alt
	}
	return
}

func productFilterToSchemaV1(
	v domain.ProductFilter,
) (s schema.ProductFilterV1) {
	s.ProductName = v.ProductName
	s.Blocked = v.Blocked
	return
}
