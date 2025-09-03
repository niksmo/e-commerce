package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/niksmo/e-commerce/internal/core/domain"
)

type ProductsRepository struct {
	sqldb sqldb
}

func NewProductsRepository(sqldb sqldb) ProductsRepository {
	return ProductsRepository{sqldb}
}

func (r ProductsRepository) StoreProducts(
	ctx context.Context, vs []domain.Product,
) (storeErr error) {
	const op = "ProductsRepository.StoreProducts"
	log := slog.With("op", op)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	tx, err := r.sqldb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: failed to begin tx: %w", op, err)
	}

	defer func() {
		if storeErr == nil {
			if err := tx.Commit(); err != nil {
				storeErr = fmt.Errorf("%s: failed to commit %w", op, err)
			}
			return
		}

		err := tx.Rollback()
		if err != nil {
			log.Error("failed to rollback tx", "err", err)
		}
	}()

	query := `
		INSERT INTO products (
			product_id, name, sku, brand, category,
			description, price_amount, price_currency,
			available_stock, tags, images, specifications, store_id
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (product_id, store_id) DO UPDATE SET
			name = EXCLUDED.name,
			sku = EXCLUDED.sku,
			brand = EXCLUDED.brand,
			category = EXCLUDED.category,
			description = EXCLUDED.description,
			price_amount = EXCLUDED.price_amount,
			price_currency = EXCLUDED.price_currency,
			available_stock = EXCLUDED.available_stock,
			tags = EXCLUDED.tags,
			images = EXCLUDED.images,
			specifications = EXCLUDED.specifications;
	`

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%s: failed to prepare stmt: %w", op, err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			log.Error("failed to close prepared stmt", "err", err)
		}
	}()

	for _, v := range vs {
		imgB, _ := json.Marshal(v.Images)
		specB, _ := json.Marshal(v.Specifications)
		_, err := stmt.ExecContext(ctx,
			v.ProductID, v.Name, v.SKU, v.Brand, v.Category,
			v.Description, v.Price.Amount, v.Price.Currency,
			v.AvailableStock, v.Tags, string(imgB), string(specB), v.StoreID,
		)
		if err != nil {
			return fmt.Errorf("%s: failed to exec: %w", op, err)
		}
	}

	return nil
}

func (r ProductsRepository) ReadProduct(
	ctx context.Context, productName string,
) (domain.Product, error) {
	const op = "ProductsRepository.ReadProduct"

	if err := ctx.Err(); err != nil {
		return domain.Product{}, err
	}

	query := `
		SELECT
			product_id, name, sku, brand, category,
			description, price_amount, price_currency,
			available_stock, tags, images, specifications, store_id
		FROM products
		WHERE name ILIKE $1 ORDER BY name ASC LIMIT 1;`

	var v domain.Product
	var tagsS string
	var imagesS string
	var specS string
	err := r.sqldb.QueryRowContext(ctx, query, productName+"%").Scan(
		&v.ProductID, &v.Name, &v.SKU, &v.Brand, &v.Category,
		&v.Description, &v.Price.Amount, &v.Price.Currency,
		&v.AvailableStock, &tagsS, &imagesS, &specS, &v.StoreID,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Product{}, fmt.Errorf("%s: %w", op, ErrNotFound)
		}
		return domain.Product{}, fmt.Errorf("%s: %w", op, err)
	}

	v.Tags = strings.Split(strings.Trim(tagsS, "{}"), ",")

	err = json.Unmarshal([]byte(imagesS), &v.Images)
	if err != nil {
		return domain.Product{}, fmt.Errorf("%s: %w", op, err)
	}

	err = json.Unmarshal([]byte(specS), &v.Specifications)
	if err != nil {
		return domain.Product{}, fmt.Errorf("%s: %w", op, err)
	}
	return v, nil
}
