package httphandler

type (
	Product struct {
		ProductID      string            `json:"product_id"`
		Name           string            `json:"name"`
		SKU            string            `json:"sku"`
		Brand          string            `json:"brand"`
		Category       string            `json:"category"`
		Description    string            `json:"description"`
		Price          ProductPrice      `json:"price"`
		AvailableStock int               `json:"available_stock"`
		Tags           []string          `json:"tags"`
		Images         []ProductImage    `json:"images"`
		Specifications map[string]string `json:"specifications"`
		StoreID        string            `json:"store_id"`
	}

	ProductPrice struct {
		Amount   float64 `json:"amount"`
		Currency string  `json:"currency"`
	}

	ProductImage struct {
		URL string `json:"url"`
		Alt string `json:"alt"`
	}
)

type FilterRule struct {
	Name    string `json:"name"`
	Blocked bool   `json:"blocked"`
}
