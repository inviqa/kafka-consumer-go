package retry

import (
	"context"
	"errors"
	"time"

	failuremodel "github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/data/retry/model"
)

type mockRepository struct {
	RetryMarkedSuccessful *model.Retry
	RetryMarkedErrored    *model.Retry
	PublishedFailure      *failuremodel.Failure
	retriesToReturn       []model.Retry
	willError             bool
}

func newMockRepository(willError bool) *mockRepository {
	return &mockRepository{willError: willError}
}

func (m *mockRepository) GetMessagesForRetry(ctx context.Context, topic string, sequence uint8, interval time.Duration) ([]model.Retry, error) {
	if m.willError {
		return nil, errors.New("oops")
	}
	return m.retriesToReturn, nil
}

func (m *mockRepository) MarkRetrySuccessful(ctx context.Context, retry model.Retry) error {
	if m.willError {
		return errors.New("oops")
	}
	m.RetryMarkedSuccessful = &retry
	return nil
}

func (m *mockRepository) MarkRetryErrored(ctx context.Context, retry model.Retry, err error) error {
	if m.willError {
		return errors.New("oops")
	}
	m.RetryMarkedErrored = &retry
	return nil
}

func (m *mockRepository) PublishFailure(ctx context.Context, failure failuremodel.Failure) error {
	if m.willError {
		return errors.New("oops")
	}
	m.PublishedFailure = &failure
	return nil
}
