package retries

import (
	"database/sql"
	"errors"

	"github.com/inviqa/kafka-consumer-go/data"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) Repository {
	return Repository{
		db: db,
	}
}

func (r Repository) PublishFailure(f data.Failure) error {
	return errors.New("not yet implemented")
}
