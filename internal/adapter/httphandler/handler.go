package httphandler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

// POST v1/products JSON [from task] (response 202 Accepted, 400 Bad request)
// GET v1/products?product_name=name Headers Authorization Basic is opt (200 OK, 204 No content)

type ProductsHandler struct {
	pSender port.ProductsSender
}

func RegisterProducts(mux *http.ServeMux, pSender port.ProductsSender) {
	h := ProductsHandler{pSender}
	mux.HandleFunc("POST /v1/products", h.PostProducts)
}

func (h ProductsHandler) PostProducts(w http.ResponseWriter, r *http.Request) {
	const op = "ProductsHandler.PostProducts"
	log := slog.With("op", op)

	var ps []Product
	err := json.NewDecoder(r.Body).Decode(&ps)
	if err != nil {
		http.Error(w, "invalid JSON data", http.StatusBadRequest)
		log.Warn("failed to parse JSON", "err", err)
		return
	}

	err = h.pSender.SendProducts(h.toDomain(ps))
	if err != nil {
		http.Error(
			w, "failed to accept products", http.StatusServiceUnavailable,
		)
		log.Error("failed to send products", "err", err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	if _, err = w.Write([]byte("Accepted")); err != nil {
		log.Error("failed to write response body", "err", err)
		return
	}

	log.Info("accepted", "nProducts", len(ps))
}

func (h ProductsHandler) toDomain(ps []Product) (domainPs []domain.Product) {
	for _, p := range ps {
		dp := domain.Product{
			ProductID:   p.ProductID,
			Name:        p.Name,
			SKU:         p.SKU,
			Brand:       p.Brand,
			Category:    p.Category,
			Description: p.Description,
			Price: domain.ProductPrice{
				Amount:   p.Price.Amount,
				Currency: p.Price.Currency,
			},
			AvailableStock: p.AvailableStock,
			Tags:           p.Tags,
			Specifications: p.Specifications,
			StoreID:        p.StoreID,
		}

		dp.Images = make([]domain.ProductImage, len(p.Images))
		for i := range p.Images {
			dp.Images[i].URL = p.Images[i].URL
			dp.Images[i].Alt = p.Images[i].Alt
		}
		domainPs = append(domainPs, dp)
	}
	return domainPs
}

// POST v1/filter/product JSON {"product_name" string, "blocked" bool} (response 200 OK, 400 Bad request)
// GET v1/recomendations Headers Authorization Basic is opt (200 OK, 204 No content)
