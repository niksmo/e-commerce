package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type sqldb interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type SQLDB struct {
	*sql.DB
}

func NewSQLDB(ctx context.Context, dsn string) (SQLDB, error) {
	const op = "SQLDB"
	log := slog.With("op", op)

	connConfig, _ := pgx.ParseConfig(dsn)
	connStr := stdlib.RegisterConnConfig(connConfig)
	db, _ := sql.Open("pgx", connStr)
	s := SQLDB{db}
	if err := s.PingContext(ctx); err != nil {
		return SQLDB{}, fmt.Errorf("%s: database is unavailable: %w", op, err)
	}
	log.Info("database is available")
	return s, nil
}

func (s SQLDB) Close() {
	const op = "SQLDB.Close"
	log := slog.With("op", op)

	log.Info("closing sql database...")

	if err := s.DB.Close(); err != nil {
		log.Error("failed to close", "err", err)
		return
	}
	log.Info("sql database is closed")
}
