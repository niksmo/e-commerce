package domain

type (
	Product struct {
		ProductID      string
		Name           string
		SKU            string
		Brand          string
		Category       string
		Description    string
		Price          ProductPrice
		AvailableStock int
		Tags           []string
		Images         []ProductImage
		Specifications map[string]string
		StoreID        string
	}

	ProductPrice struct {
		Amount   float64
		Currency string
	}

	ProductImage struct {
		URL string
		Alt string
	}
)

type ProductFilter struct {
	ProductName string
	Blocked     bool
}
