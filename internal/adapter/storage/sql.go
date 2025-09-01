package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var _ port.ProductsStorage = (*SQLStorage)(nil)

type sqldb interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Close() error
}

type SQLStorage struct {
	sqldb sqldb
}

func NewSQLStorage(ctx context.Context, dsn string) (SQLStorage, error) {
	connConfig, _ := pgx.ParseConfig(dsn)
	connStr := stdlib.RegisterConnConfig(connConfig)
	db, _ := sql.Open("pgx", connStr)

	s := SQLStorage{db}
	if err := s.ping(ctx); err != nil {
		return SQLStorage{}, err
	}
	return s, nil
}

func (s SQLStorage) ping(ctx context.Context) error {
	const op = "SQLStorage.ping"
	if err := s.sqldb.PingContext(ctx); err != nil {
		return fmt.Errorf("%s: database unavailable: %w", op, err)
	}
	slog.Info("database is available", "op", op)
	return nil
}

func (s SQLStorage) StoreProducts(
	ctx context.Context, ps []domain.Product,
) error {
	const op = "SQLStorage.StoreProducts"
	log := slog.With("op", op)
	log.Info("MOCK upsert successfull")
	return nil
}
