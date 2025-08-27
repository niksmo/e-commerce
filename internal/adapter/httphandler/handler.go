package httphandler

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

// TODO: GET v1/products?product_name=name Headers Authorization Basic is opt (200 OK, 204 No content)
// TODO: GET v1/recomendations Headers Authorization Basic is opt (200 OK, 204 No content)

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
	if !decodeJSON(w, r.Body, log, &ps) {
		return
	}

	domainPs := h.productsToDomain(ps)
	err := h.pSender.SendProducts(r.Context(), domainPs)
	if err != nil {
		http.Error(
			w, "failed to accept products", http.StatusServiceUnavailable,
		)
		log.Error("failed to send products", "err", err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, err = w.Write([]byte("Accepted"))
	if err != nil {
		log.Error("failed to write response body", "err", err)
		return
	}

	log.Info("accepted", "nProducts", len(ps))
}

func (h ProductsHandler) productsToDomain(
	ps []Product,
) (domainPs []domain.Product) {
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

type FilterHandler struct {
	pFilter port.ProductsFilter
}

func RegisterFilter(mux *http.ServeMux, pFilter port.ProductsFilter) {
	h := FilterHandler{pFilter}

	mux.HandleFunc("POST /v1/filter/products", h.PostProductsRule)
}

func (h FilterHandler) PostProductsRule(
	w http.ResponseWriter, r *http.Request,
) {
	const op = "FilterHandler.PostProductsRule"
	log := slog.With("op", op)

	var rule FilterRule
	if !decodeJSON(w, r.Body, log, &rule) {
		return
	}

	pf := h.toProductFilter(rule)
	err := h.pFilter.SetRule(r.Context(), pf)
	if err != nil {
		http.Error(
			w, "failed to set filter rule", http.StatusServiceUnavailable,
		)
		log.Error(
			"failed to set product filter rule",
			"productName", pf.ProductName,
			"blocked", pf.Blocked,
		)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte("Rule applied"))

	log.Info("rule applied", "productName", pf.ProductName, "blocked", pf.Blocked)
}

func (h FilterHandler) toProductFilter(
	r FilterRule,
) (pf domain.ProductFilter) {
	pf.ProductName = r.Name
	pf.Blocked = r.Blocked
	return
}

func decodeJSON(
	w http.ResponseWriter, r io.Reader, log *slog.Logger, v any,
) (ok bool) {
	err := json.NewDecoder(r).Decode(&v)
	if err != nil {
		http.Error(w, "invalid JSON data", http.StatusBadRequest)
		log.Warn("failed to parse JSON", "err", err)
		return false
	}
	return true
}
