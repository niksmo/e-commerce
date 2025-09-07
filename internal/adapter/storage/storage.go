package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"time"

	hdfscl "github.com/colinmarc/hdfs/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/niksmo/e-commerce/pkg/retry"
)

var (
	ErrNotFound = errors.New("not found")
)

type sqldb interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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

type hdfsStorage interface {
	Stat(name string) (os.FileInfo, error)
	Create(name string) (*hdfscl.FileWriter, error)
	Append(name string) (*hdfscl.FileWriter, error)
	Filepaths(dir string) (paths []string)
}

type HDFS struct {
	*hdfscl.Client
}

func NewHDFS(addr, user string) (HDFS, error) {
	const op = "HDFS"
	log := slog.With("op", op)

	cl, err := hdfscl.NewClient(
		hdfscl.ClientOptions{
			Addresses:           []string{addr},
			User:                user,
			UseDatanodeHostname: true,
		},
	)
	if err != nil {
		return HDFS{}, fmt.Errorf("%s: %w", op, err)
	}

	retryCfg := retry.RetryConfig{
		MaxAttempts: 10,
		Backoff:     retry.LineareBackoff(5 * time.Second),
	}
	retry.Do(context.TODO(), retryCfg, func() error {
		if _, err := cl.StatFs(); err != nil {
			return fmt.Errorf("%s: HDFS is unavailable: %w", op, err)
		}
		return nil
	})
	log.Info("HDFS is available")

	return HDFS{cl}, nil
}

func (s HDFS) Close() {
	const op = "HDFS.Close"

	log := slog.With("op", op)

	log.Info("closing HDFS...")

	if err := s.Client.Close(); err != nil {
		log.Error("failed to close", "err", err)
		return
	}
	log.Info("HDFS is closed")
}

func (s HDFS) Filepaths(dir string) (paths []string) {
	const op = "HDFS.Filepaths"
	log := slog.With("op", op)

	s.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if path == dir {
			return nil
		}

		if err != nil {
			log.Error("failed on dir walk", "err", err)
			return nil
		}

		if info.IsDir() {
			return fs.SkipDir
		}

		paths = append(paths, path)
		return nil
	})

	return paths
}
