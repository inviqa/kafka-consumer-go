package internal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries/model"
)

const (
	consideredStaleAfter = time.Minute * 10
)

var (
	// TODO: any columns (and struct fields) that can be removed from app code?
	columns = []string{"id", "topic", "payload_json", "payload_headers", "payload_key", "kafka_offset", "kafka_partition", "attempts"}
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) Repository {
	return Repository{
		db: db,
	}
}

func (r Repository) PublishFailure(ctx context.Context, f data.Failure) error {
	q := `INSERT INTO kafka_consumer_retries(topic, payload_json, payload_headers, kafka_offset, kafka_partition, payload_key) VALUES($1, $2, $3, $4, $5, $6);`
	_, err := r.db.ExecContext(ctx, q, f.Topic, f.Message, f.MessageHeaders, f.KafkaOffset, f.KafkaPartition, f.MessageKey)
	if err != nil {
		return fmt.Errorf("data/retries: error publishing failure to the database: %w", err)
	}
	return nil
}

func (r Repository) GetMessagesForRetry(ctx context.Context, topic string, sequence uint8, interval time.Duration) ([]model.Retry, error) {
	batchId := uuid.New()
	stale := time.Now().Add(consideredStaleAfter * -1)
	before := time.Now().Add(interval * -1)

	// TODO: add an index for this WHERE condition
	upSql := `UPDATE kafka_consumer_retries SET batch_id = $1, retry_started_at = NOW()
		WHERE id IN(
			SELECT id FROM kafka_consumer_retries
			WHERE topic = $2
		    AND (
				(batch_id IS NULL AND retry_started_at IS NULL) OR 
				(batch_id IS NOT NULL AND retry_finished_at IS NULL AND retry_started_at < $3)
			)
			AND attempts = $4 AND deadlettered = false AND successful = false AND updated_at <= $5
			LIMIT 250
		);`

	_, err := r.db.ExecContext(ctx, upSql, batchId, topic, stale, sequence, before)
	if err != nil {
		return nil, fmt.Errorf("data/retries: error updating retries records when creating a batch: %w", err)
	}

	q := fmt.Sprintf(`SELECT %s FROM kafka_consumer_retries WHERE batch_id = $1`, columns)

	// #nosec G201
	rows, err := r.db.QueryContext(ctx, q, batchId)
	if err != nil {
		return nil, fmt.Errorf("data/retries: error getting messages for retry: %w", err)
	}
	defer rows.Close()

	var retries []model.Retry
	for rows.Next() {
		retry := model.Retry{}
		err := rows.Scan(&retry.ID, &retry.Topic, &retry.PayloadJSON, &retry.PayloadHeaders, &retry.PayloadKey, &retry.KafkaOffset, &retry.KafkaPartition, &retry.Attempts)
		if err != nil {
			return nil, fmt.Errorf("data/retries: error scanning result into memory: %w", err)
		}
		retries = append(retries, retry)
	}

	return retries, nil
}

func (r Repository) MarkRetrySuccessful(ctx context.Context, retry model.Retry) error {
	q := `UPDATE kafka_consumer_retries
		SET attempts = $1, last_error = '', retry_finished_at = NOW(), errored = false, successful = true, updated_at = NOW()
		WHERE id = $2;`

	_, err := r.db.ExecContext(ctx, q, retry.Attempts, retry.ID)
	if err != nil {
		return fmt.Errorf("data/retries: error marking a retry as successful: %w", err)
	}

	return nil
}

func (r Repository) MarkRetryErrored(ctx context.Context, retry model.Retry, retryErr error) error {
	q := `UPDATE kafka_consumer_retries
		SET batch_id = NULL, attempts = $1, last_error = $2, retry_finished_at = NOW(), errored = $3, deadlettered = $4, updated_at = NOW()
		WHERE id = $5;`

	_, err := r.db.ExecContext(ctx, q, retry.Attempts, retryErr.Error(), retry.Errored, retry.Deadlettered, retry.ID)
	if err != nil {
		return fmt.Errorf("data/retries: error marking a retry as errored: %w", err)
	}

	return nil
}
