package retry

import (
	"context"
	"database/sql"
	"time"

	"github.com/inviqa/kafka-consumer-go/config"
	failuremodel "github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/data/retry/internal"
	"github.com/inviqa/kafka-consumer-go/data/retry/model"
)

type Manager struct {
	dbRetries config.DBRetries
	repo      repository
}

type repository interface {
	GetMessagesForRetry(ctx context.Context, topic string, sequence uint8, interval time.Duration) ([]model.Retry, error)
	MarkRetrySuccessful(ctx context.Context, retry model.Retry) error
	MarkRetryErrored(ctx context.Context, retry model.Retry, err error) error
	PublishFailure(ctx context.Context, failure failuremodel.Failure) error
}

func NewManagerWithDefaults(dbRetries config.DBRetries, db *sql.DB) *Manager {
	return &Manager{
		dbRetries: dbRetries,
		repo:      internal.NewRepository(db),
	}
}

func (m Manager) GetBatch(ctx context.Context, topic string, sequence uint8, interval time.Duration) ([]model.Retry, error) {
	return m.repo.GetMessagesForRetry(ctx, topic, sequence, interval)
}

func (m Manager) MarkSuccessful(ctx context.Context, retry model.Retry) error {
	return m.repo.MarkRetrySuccessful(ctx, m.dbRetries.MakeRetrySuccessful(retry))
}

func (m Manager) MarkErrored(ctx context.Context, retry model.Retry, err error) error {
	return m.repo.MarkRetryErrored(ctx, m.dbRetries.MakeRetryErrored(retry), err)
}

func (m Manager) PublishFailure(ctx context.Context, failure failuremodel.Failure) error {
	return m.repo.PublishFailure(ctx, failure)
}
