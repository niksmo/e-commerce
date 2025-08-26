package schema

const ProductSchemaTextV1 = `{
	"type": "record",
	"namespace": "products",
	"name": "product",
	"fields" : [
		{"name": "product_id"", "type": "string"},
		{"name": "name"", "type": "string"},
		{"name": "sku"", "type": "string"},
		{"name": "brand"", "type": "string"},
		{"name": "category"", "type": "string"},
		{"name": "description"", "type": "string"},
		{"name": "price"", "type": "****"},
		{"name": "available_stock"", "type": "long"},
		{"name": "tags"", "type": "****"},
		{"name": "images"", "type": "****"},
		{"name": "specifications"", "type": "****"},
		{"name": "store_id"", "type": "string"}
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
