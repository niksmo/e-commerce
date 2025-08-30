package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/spf13/pflag"
)

const (
	storagePathFlag   = "storage-path"
	migrationPathFlag = "migrations-path"
)

func main() {
	storagePath, migrationsPath := getFlagsValues()
	validateFlags(storagePath, migrationsPath)
	makeMigrations(storagePath, migrationsPath)
}

type MigrationLogger struct {
	logger  *slog.Logger
	verbose bool
}

func NewMigrationLogger() *MigrationLogger {
	return &MigrationLogger{
		logger:  slog.Default(),
		verbose: true,
	}
}

func (ml *MigrationLogger) Printf(format string, v ...any) {
	ml.logger.Info(fmt.Sprintf(format, v...))
}

func (ml *MigrationLogger) Verbose() bool {
	return ml.verbose
}

func getFlagsValues() (storage, migrations string) {
	storagePath := pflag.StringP(storagePathFlag, "s", "", "")
	migrationsPath := pflag.StringP(migrationPathFlag, "m", "", "")
	pflag.Parse()
	return *storagePath, *migrationsPath
}

func validateFlags(storagePath, migrationsPath string) {
	var errs []error

	if storagePath == "" {
		errs = append(errs, fmt.Errorf("--%s flag: required", storagePathFlag))
	}

	if migrationsPath == "" {
		errs = append(errs, fmt.Errorf("--%s flag: required", migrationPathFlag))
	}

	if len(errs) != 0 {
		slog.Error("too few args", "err", errors.Join(errs...))
		fallDown()
	}
}

func makeMigrations(storagePath, migrationsPath string) {

	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationsPath),
		fmt.Sprintf("pgx5://%s", storagePath),
	)
	if err != nil {
		slog.Error("failed to migrate", "err", err)
		fallDown()
	}

	m.Log = NewMigrationLogger()

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			m.Log.Printf("no migrations to apply")
			return
		}
		slog.Error("failed to migrate", "err", err)
		fallDown()
	}
	m.Log.Printf("migration applied\n")
}

func fallDown() {
	os.Exit(2)
}
