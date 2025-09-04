package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/pkg/retry"
)

type (
	clientEvent struct {
		Username       string            `json:"username"`
		ProductName    string            `json:"product_name"`
		Brand          string            `json:"brand"`
		Category       string            `json:"category"`
		Price          productPrice      `json:"price"`
		Tags           []string          `json:"tags"`
		Specifications map[string]string `json:"specifications"`
		StoreID        string            `json:"store_id"`
	}

	productPrice struct {
		Amount   float64 `json:"amount"`
		Currency string  `json:"currency"`
	}
)

type ClientEventsRepository struct {
	hdfs hdfsStorage
}

func NewClientEventsRepository(hdfs hdfsStorage) ClientEventsRepository {
	return ClientEventsRepository{hdfs}
}

func (r ClientEventsRepository) StoreEvents(
	ctx context.Context, username string, evts []domain.ClientFindProductEvent,
) error {
	const op = "ClientEventsRepository.StoreEvents"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	filepath := r.getFileName(username)

	w, err := r.createWriter(filepath)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = r.saveEvents(w, evts)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = r.closeWriter(ctx, w)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r ClientEventsRepository) getFileName(username string) string {
	return "/" + username
}

func (r ClientEventsRepository) createWriter(filepath string) (io.WriteCloser, error) {
	w, err := r.hdfs.Append(filepath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		w, err = r.hdfs.Create(filepath)
		if err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (r ClientEventsRepository) saveEvents(
	w io.WriteCloser, evts []domain.ClientFindProductEvent,
) error {
	for _, evt := range evts {
		v := r.toClientEvent(evt)
		err := json.NewEncoder(w).Encode(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r ClientEventsRepository) closeWriter(ctx context.Context, w io.WriteCloser) error {
	retryCfg := retry.RetryConfig{
		MaxAttempts: 5,
		Backoff:     retry.LineareBackoff(50 * time.Millisecond),
		ShouldRetry: func(err error) bool {
			return errors.Is(err, hdfs.ErrReplicating)
		},
	}

	err := retry.Do(ctx, retryCfg, w.Close)
	if err != nil {
		return err
	}

	return nil
}

func (r ClientEventsRepository) toClientEvent(
	evt domain.ClientFindProductEvent,
) (v clientEvent) {
	v.Username = evt.Username
	v.Brand = evt.Brand
	v.ProductName = evt.ProductName
	v.Brand = evt.Brand
	v.Category = evt.Category
	v.Price.Amount = evt.Price.Amount
	v.Price.Currency = evt.Price.Currency
	v.Tags = evt.Tags
	v.Specifications = evt.Specifications
	v.StoreID = evt.StoreID
	return
}
