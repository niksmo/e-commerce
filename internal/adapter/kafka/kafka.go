package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
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

// A MakeTLSConfig returns [*tls.Config].
//
// All args are the filepaths.
func MakeTLSConfig(ca, cert, key string) *tls.Config {
	const op = "kafka.MakeTLSConfig"

	caCert, err := os.ReadFile(ca)
	if err != nil {
		err = fmt.Errorf("%s: failed to read CA certificate file: %w", op, err)
		panic(err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		err = fmt.Errorf("%s: %s", op, "failed to parse CA certificate")
		panic(err)
	}

	clientCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		panic(err)
	}

	return &tls.Config{
		RootCAs:      caCertPool,
		ClientCAs:    caCertPool,
		Certificates: []tls.Certificate{clientCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
}

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

func schemaV1ToProduct(s schema.ProductV1) (p domain.Product) {
	p.ProductID = s.ProductID
	p.Name = s.Name
	p.SKU = s.SKU
	p.Brand = s.Brand
	p.Category = s.Category
	p.Description = s.Description
	p.Price.Amount = s.Price.Amount
	p.Price.Currency = s.Price.Currency
	p.AvailableStock = s.AvailableStock
	p.Tags = s.Tags
	p.Specifications = s.Specifications
	p.StoreID = s.StoreID

	p.Images = make([]domain.ProductImage, len(s.Images))
	for i := range s.Images {
		p.Images[i].URL = s.Images[i].URL
		p.Images[i].Alt = s.Images[i].Alt
	}
	return p
}

func clientEventToSchema(
	evt domain.ClientFindProductEvent,
) (s schema.ClientFindProductEventV1) {
	s.Username = evt.Username
	s.ProductName = evt.ProductName
	s.Brand = evt.Brand
	s.Category = evt.Category
	s.Price.Amount = evt.Price.Amount
	s.Price.Currency = evt.Price.Currency
	s.Tags = evt.Tags
	s.Specifications = evt.Specifications
	s.StoreID = evt.StoreID
	return
}

func schemaV1ToClientEvent(
	s schema.ClientFindProductEventV1,
) (evt domain.ClientFindProductEvent) {
	evt.Username = s.Username
	evt.ProductName = s.ProductName
	evt.Brand = s.Brand
	evt.Category = s.Category
	evt.Price.Amount = s.Price.Amount
	evt.Price.Currency = s.Price.Currency
	evt.Tags = s.Tags
	evt.Specifications = s.Specifications
	evt.StoreID = s.StoreID
	return
}
