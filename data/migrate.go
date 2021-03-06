package data

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func MigrateDatabase(db *sql.DB, schema string) error {
	databaseDriver, err := postgres.WithInstance(db, &postgres.Config{MigrationsTable: "kafka_consumer_migrations"})
	if err != nil {
		return fmt.Errorf("unable to create migration instance from database: %w", err)
	}

	d, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("unable to load migration files from embedded filesystem: %w", err)
	}

	m, err := migrate.NewWithInstance("go-bindata", d, schema, databaseDriver)
	if err != nil {
		return fmt.Errorf("failed to load migration files from source driver: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to migrate database: %w", err)
	}

	return nil
}
