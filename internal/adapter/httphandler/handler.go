package httphandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
	"github.com/niksmo/e-commerce/internal/core/service"
)

type ProductsHandler struct {
	pSender port.ProductsSender
	pFinder port.ProductFinder
	oViewer port.ProductOffersViewer
}

func RegisterProducts(
	mux *http.ServeMux,
	pSender port.ProductsSender,
	pFinder port.ProductFinder,
	oViewer port.ProductOffersViewer,
) {
	h := ProductsHandler{pSender, pFinder, oViewer}
	mux.HandleFunc("GET /v1/products", h.GetProduct)
	mux.HandleFunc("POST /v1/products", h.PostProducts)
	mux.HandleFunc("GET /v1/products/offers", h.GetOffers)
}

func (h ProductsHandler) GetProduct(w http.ResponseWriter, r *http.Request) {
	const (
		op          = "ProductsHandler.GetProduct"
		maxNameSize = 100
	)
	log := slog.With("op", op)

	username, _ := getUsername(r.Context())

	productName := r.FormValue("name")

	if productName == "" {
		errMsg := "product name is not provided"
		statusCode := http.StatusBadRequest
		http.Error(w, errMsg, statusCode)
		log.Warn(errMsg, "statusCode", statusCode)
		return
	}

	if len(productName) > maxNameSize {
		productName = productName[:maxNameSize]
	}

	v, err := h.pFinder.FindProduct(r.Context(), productName, username)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrNotFound):
			statusCode := http.StatusNoContent
			http.Error(w, "", statusCode)
			log.Info("not found",
				"productName", productName,
				"statusCode", statusCode)
		default:
			errMsg := "failed to find product"
			statusCode := http.StatusServiceUnavailable
			http.Error(w, errMsg, statusCode)
			log.Error(errMsg, "statusCode", statusCode, "err", err)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	if !encodeJSON(w, domainToProduct(v), log) {
		return
	}
	log.Info("find",
		"statusCode", http.StatusOK,
		"productName", v.Name, "user", username)
}

func (h ProductsHandler) GetOffers(w http.ResponseWriter, r *http.Request) {
	const op = "ProductsHandler.GetOffers"
	log := slog.With("op", op)

	log.Info("try get offers")
	offers, err := h.oViewer.ViewOffers(r.Context())
	if err != nil {
		log.Error("failed get offers", "err", err)
		return
	}
	fmt.Println("OFFERS***:", offers)
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
	_, err = w.Write([]byte("Products list accepted"))
	if err != nil {
		log.Error("failed to write response body", "err", err)
		return
	}

	log.Info("accepted", "nProducts", len(ps))
}

func (h ProductsHandler) productsToDomain(
	vs []Product,
) (products []domain.Product) {
	for _, v := range vs {
		products = append(products, productToDomain(v))
	}
	return products
}

type FilterHandler struct {
	pFilter port.ProductFilterSetter
}

func RegisterFilter(mux *http.ServeMux, pFilter port.ProductFilterSetter) {
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

	fv := productFilterToDomain(rule)

	err := h.pFilter.SetRule(r.Context(), fv)
	if err != nil {
		http.Error(
			w, "failed to set filter rule", http.StatusServiceUnavailable,
		)
		log.Error(
			"failed to set product filter rule",
			"productName", fv.ProductName,
			"blocked", fv.Blocked,
		)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte("Rule accepted"))
	if err != nil {
		log.Error("failed to write response body", "err", err)
		return
	}

	log.Info(
		"product filter rule accepted",
		"productName", fv.ProductName, "blocked", fv.Blocked,
	)
}

func getUsername(ctx context.Context) (string, bool) {
	const op = "httphandler.getUsername"
	log := slog.With("op", op)

	v := ctx.Value(contextUserKey)
	if v == nil {
		return "", false
	}
	username, ok := v.(string)
	if !ok {
		log.Warn(
			"unexpected context value type",
			"valueType", fmt.Sprintf("%T", v),
		)
		return "", false
	}
	return username, true
}

func decodeJSON(
	w http.ResponseWriter, r io.Reader, log *slog.Logger, v any,
) (ok bool) {
	err := json.NewDecoder(r).Decode(&v)
	if err != nil {
		http.Error(w, "invalid JSON data", http.StatusBadRequest)
		log.Warn("failed to read JSON data", "err", err)
		return false
	}
	return true
}

func encodeJSON(
	w http.ResponseWriter, v any, log *slog.Logger,
) (ok bool) {
	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		log.Error("failed to write JSON data", "err", err)
		return false
	}
	return true
}

func productFilterToDomain(r FilterRule) domain.ProductFilter {
	return domain.ProductFilter{
		ProductName: r.Name,
		Blocked:     r.Blocked,
	}
}

func productToDomain(v Product) (p domain.Product) {
	p.ProductID = v.ProductID
	p.Name = v.Name
	p.SKU = v.SKU
	p.Brand = v.Brand
	p.Category = v.Category
	p.Description = v.Description
	p.Price.Amount = v.Price.Amount
	p.Price.Currency = v.Price.Currency
	p.AvailableStock = v.AvailableStock
	p.Tags = v.Tags
	p.Specifications = v.Specifications
	p.StoreID = v.StoreID

	p.Images = make([]domain.ProductImage, len(v.Images))
	for i := range v.Images {
		p.Images[i].URL = v.Images[i].URL
		p.Images[i].Alt = v.Images[i].Alt
	}
	return
}

func domainToProduct(p domain.Product) (v Product) {
	v.ProductID = p.ProductID
	v.Name = p.Name
	v.SKU = p.SKU
	v.Brand = p.Brand
	v.Category = p.Category
	v.Description = p.Description
	v.Price.Amount = p.Price.Amount
	v.Price.Currency = p.Price.Currency
	v.AvailableStock = p.AvailableStock
	v.Tags = p.Tags
	v.Specifications = p.Specifications
	v.StoreID = p.StoreID

	v.Images = make([]ProductImage, len(p.Images))
	for i := range p.Images {
		v.Images[i].URL = p.Images[i].URL
		v.Images[i].Alt = p.Images[i].Alt
	}
	return
}
