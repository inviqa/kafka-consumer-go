package deadletter

import (
	"context"
	"database/sql"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) Repository {
	return Repository{
		db: db,
	}
}

func (r Repository) Count(ctx context.Context) uint {
	var c uint

	row := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM kafka_consumer_retries WHERE deadlettered = true;`)
	if err := row.Scan(&c); err != nil {
		return 0
	}

	return c
}
