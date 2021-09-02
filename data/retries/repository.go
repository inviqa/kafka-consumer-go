package retries

import (
	"database/sql"
	"fmt"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
)

type Repository struct {
	// TODO: remove??
	cfg *config.Config
	db  *sql.DB
}

func NewRepository(cfg *config.Config, db *sql.DB) Repository {
	return Repository{
		cfg: cfg,
		db:  db,
	}
}

func (r Repository) PublishFailure(f data.Failure) error {
	q := `INSERT INTO kafka_consumer_retries(topic, payload_json, payload_headers, kafka_offset, kafka_partition, payload_key) VALUES($1, $2, $3, $4, $5, $6);`
	_, err := r.db.Exec(q, f.Topic, f.Message, f.MessageHeaders, f.KafkaOffset, f.KafkaPartition, f.MessageKey)
	if err != nil {
		return fmt.Errorf("data/retries: error publishing failure to the database: %w", err)
	}
	return nil
}
