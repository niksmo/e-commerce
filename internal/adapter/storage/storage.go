package storage

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type sqldb interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Close() error
}

type sqlStorage struct {
	sqldb
}

func newSQLStorage(ctx context.Context, dsn string) (sqlStorage, error) {
	connConfig, _ := pgx.ParseConfig(dsn)
	connStr := stdlib.RegisterConnConfig(connConfig)
	db, _ := sql.Open("pgx", connStr)
	s := sqlStorage{db}
	if err := s.PingContext(ctx); err != nil {
		return sqlStorage{}, err
	}
	return s, nil
}
