package retries

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
)

var (
	// TODO: any columns (and struct fields) that can be removed from app code?
	columns = []string{"id", "topic", "payload_json", "payload_headers", "payload_key", "kafka_offset", "kafka_partition", "attempts", "last_error", "created_at", "updated_at"}
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

func (r Repository) GetMessagesForRetry(topic string, sequence uint8, interval time.Duration) ([]Retry, error) {
	// TODO: should we add batch creation so that multiple consumers can run safely and not process the same message twice??

	before := time.Now().Add(interval * -1)
	// TODO: add an index for this WHERE condition
	q := fmt.Sprintf(`SELECT %s FROM kafka_consumer_retries WHERE topic = $1 AND attempts = $2 AND deadlettered = false AND successful = false AND updated_at <= $3`, columns)

	rows, err := r.db.Query(q, topic, sequence, before)
	if err != nil {
		return nil, fmt.Errorf("data/retries: error getting messages for retry: %w", err)
	}
	defer rows.Close()

	var retries []Retry
	for rows.Next() {
		retry := Retry{}
		err := rows.Scan(&retry.ID, &retry.Topic, &retry.PayloadJSON, &retry.PayloadHeaders, &retry.PayloadKey, &retry.KafkaOffset, &retry.KafkaPartition, &retry.Attempts, &retry.LastError, &retry.CreatedAt, &retry.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("data/retries: error scanning result into memory: %w", err)
		}
		retries = append(retries, retry)
	}

	return retries, nil
}

func (r Repository) MarkRetrySuccessful(id int64) error {
	panic("implement me")
}

func (r Repository) MarkRetryErrored(retry Retry) error {
	panic("implement me")
}
