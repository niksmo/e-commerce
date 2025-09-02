package schema

import (
	_ "embed"
)

//go:embed product_v1.avsc
var ProductSchemaTextV1 string

type (
	ProductV1 struct {
		ProductID      string            `avro:"product_id"`
		Name           string            `avro:"name"`
		SKU            string            `avro:"sku"`
		Brand          string            `avro:"brand"`
		Category       string            `avro:"category"`
		Description    string            `avro:"description"`
		Price          ProductPriceV1    `avro:"price"`
		AvailableStock int               `avro:"available_stock"`
		Tags           []string          `avro:"tags"`
		Images         []ProductImageV1  `avro:"images"`
		Specifications map[string]string `avro:"specifications"`
		StoreID        string            `avro:"store_id"`
	}

	ProductPriceV1 struct {
		Amount   float64 `avro:"amount"`
		Currency string  `avro:"currency"`
	}

	ProductImageV1 struct {
		URL string `avro:"url"`
		Alt string `avro:"alt"`
	}
)

//go:embed product_filter_v1.avsc
var ProductFilterSchemaTextV1 string

type ProductFilterV1 struct {
	ProductName string `avro:"product_name"`
	Blocked     bool   `avro:"blocked"`
}

//go:embed client_find_product_event_v1.avsc
var ClientFindProductSchemaTextV1 string

type (
	ClientFindProductEventV1 struct {
		Username       string            `avro:"username"`
		ProductName    string            `avro:"product_name"`
		Brand          string            `avro:"brand"`
		Category       string            `avro:"category"`
		Description    string            `avro:"description"`
		Price          ProductPriceV1    `avro:"price"`
		Tags           []string          `avro:"tags"`
		Specifications map[string]string `avro:"specifications"`
		StoreID        string            `avro:"store_id"`
	}
)
