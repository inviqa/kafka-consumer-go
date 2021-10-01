package internal

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries/model"
)

var (
	// TODO: any columns (and struct fields) that can be removed from app code?
	columns = []string{"id", "topic", "payload_json", "payload_headers", "payload_key", "kafka_offset", "kafka_partition", "attempts", "last_error", "created_at", "updated_at"}
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
	q := `INSERT INTO kafka_consumer_retries(topic, payload_json, payload_headers, kafka_offset, kafka_partition, payload_key) VALUES($1, $2, $3, $4, $5, $6);`
	_, err := r.db.Exec(q, f.Topic, f.Message, f.MessageHeaders, f.KafkaOffset, f.KafkaPartition, f.MessageKey)
	if err != nil {
		return fmt.Errorf("data/retries: error publishing failure to the database: %w", err)
	}
	return nil
}

func (r Repository) GetMessagesForRetry(topic string, sequence uint8, interval time.Duration) ([]model.Retry, error) {
	// TODO: should we add batch creation so that multiple consumers can run safely and not process the same message twice??

	batchId := uuid.New()
	stale := time.Now().Add(time.Duration(-10) * time.Minute) // TODO: make this configurable??
	before := time.Now().Add(interval * -1)

	// TODO: add an index for this WHERE condition
	upSql := `UPDATE kafka_consumer_retries SET batch_id = $1, push_started_at = NOW()
		WHERE id IN(
			SELECT id FROM kafka_consumer_retries
			WHERE topic = $2
		    AND (
				(batch_id IS NULL AND retry_started_at IS NULL) OR 
				(batch_id IS NOT NULL AND retry_completed_at IS NULL AND retry_started_at < $3)
			)
			AND attempts = $4 AND deadlettered = false AND successful = false AND updated_at <= $5
			LIMIT 250
		);`


	_, err := r.db.Exec(upSql, batchId, topic, stale, sequence, before)
	if err != nil {
		return nil, fmt.Errorf("data/retries: error updating retries records when creating a batch: %w", err)
	}

	q := fmt.Sprintf(`SELECT %s FROM kafka_consumer_retries WHERE batch_id = $1`, columns)

	rows, err := r.db.Query(q, batchId)
	if err != nil {
		return nil, fmt.Errorf("data/retries: error getting messages for retry: %w", err)
	}
	defer rows.Close()

	var retries []model.Retry
	for rows.Next() {
		retry := model.Retry{}
		err := rows.Scan(&retry.ID, &retry.Topic, &retry.PayloadJSON, &retry.PayloadHeaders, &retry.PayloadKey, &retry.KafkaOffset, &retry.KafkaPartition, &retry.Attempts, &retry.LastError, &retry.CreatedAt, &retry.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("data/retries: error scanning result into memory: %w", err)
		}
		retries = append(retries, retry)
	}

	return retries, nil
}

func (r Repository) MarkRetrySuccessful(id int64) error {
	return errors.New("not implemented")
}

func (r Repository) MarkRetryErrored(retry model.Retry, err error) error {
	//q := `UPDATE kafka_consumer_retries SET attempts = $1, last_error = $2`

	return errors.New("not implemented")
}

func (r Repository) MarkRetryDeadLettered(retry model.Retry, err error) error {
	return errors.New("not implemented")
}
