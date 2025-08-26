package schema

import (
	"fmt"

	"github.com/hamba/avro/v2"
)

const ProductSchemaTextV1 = `{
	"type": "record",
	"namespace": "products",
	"name": "product",
	"fields" : [
		{"name": "product_id", "type": "string"},
		{"name": "name", "type": "string"},
		{"name": "sku", "type": "string"},
		{"name": "brand", "type": "string"},
		{"name": "category", "type": "string"},
		{"name": "description", "type": "string"},
		{"name": "price", "type": {
			"type": "record",
			"name": "product_price",
			"fields": [
		    	{"name": "amount", "type": "double"},
				{"name": "currency", "type": "string"}
			]
		}},
		{"name": "available_stock", "type": "long"},
		{"name": "tags", "type": {
			"type": "array",
			"items": "string"
		}},
		{"name": "images", "type": {
			"type": "array",
			"items": {
				"type": "record",
		    	"name": "product_image",
				"fields": [
			  		{"name": "url", "type": "string"},
			  		{"name": "alt", "type": "string"}
			]}
		}},
		{"name": "specifications", "type": {
		  "type": "map",
		  "values": "string"
		}},
		{"name": "store_id", "type": "string"}
	]
}`

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

func ProductV1Avro() avro.Schema {
	s, err := avro.Parse(ProductSchemaTextV1)
	if err != nil {
		err = fmt.Errorf(
			"failed to parse ProductSchemaTextV1,"+
				" contact with package dev team: %w",
			err,
		)
		panic(err)
	}
	return s
}

func ProductV1AvroEncodeFn() func(v any) ([]byte, error) {
	return func(v any) ([]byte, error) {
		s := ProductV1Avro()
		return avro.Marshal(s, v)
	}
}

func ProductV1AvroDecodeFn() func([]byte, any) error {
	return func(data []byte, v any) error {
		s := ProductV1Avro()
		return avro.Unmarshal(s, data, v)
	}
}
