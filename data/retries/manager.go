package retries

import (
	"database/sql"
	"time"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries/internal"
	"github.com/inviqa/kafka-consumer-go/data/retries/model"
)

type Manager struct {
	dbRetries config.DBRetries
	repo      repository
}

type repository interface {
	GetMessagesForRetry(topic string, sequence uint8, interval time.Duration) ([]model.Retry, error)
	MarkRetrySuccessful(id int64) error
	MarkRetryErrored(retry model.Retry, err error) error
	MarkRetryDeadLettered(retry model.Retry, err error) error
	PublishFailure(failure data.Failure) error
}

func NewManagerWithDefaults(dbRetries config.DBRetries, db *sql.DB) *Manager {
	return &Manager{
		dbRetries: dbRetries,
		repo:      internal.NewRepository(db),
	}
}

func (m Manager) GetBatch(topic string, sequence uint8, interval time.Duration) ([]model.Retry, error) {
	return m.repo.GetMessagesForRetry(topic, sequence, interval)
}

func (m Manager) MarkSuccessful(id int64) error {
	return m.repo.MarkRetrySuccessful(id)
}

func (m Manager) MarkErrored(retry model.Retry, err error) error {
	// TODO: call repo.MarkDeadLettered() if necessary
	return m.repo.MarkRetryErrored(retry, err)
}

func (m Manager) PublishFailure(failure data.Failure) error {
	return m.repo.PublishFailure(failure)
}
