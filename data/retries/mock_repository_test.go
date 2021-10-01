package retries

import (
	"context"
	"errors"
	"time"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries/model"
)

type mockRepository struct {
	retriesToReturn []model.Retry
	willError       bool
}

func newMockRepository(willError bool) *mockRepository {
	return &mockRepository{willError: willError}
}

func (m mockRepository) GetMessagesForRetry(ctx context.Context, topic string, sequence uint8, interval time.Duration) ([]model.Retry, error) {
	if m.willError {
		return nil, errors.New("oops")
	}
	return m.retriesToReturn, nil
}

func (m mockRepository) MarkRetrySuccessful(ctx context.Context, retry model.Retry) error {
	panic("implement me")
}

func (m mockRepository) MarkRetryErrored(ctx context.Context, retry model.Retry, err error) error {
	panic("implement me")
}

func (m mockRepository) PublishFailure(ctx context.Context, failure data.Failure) error {
	panic("implement me")
}
