package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries/model"
)

type mockRetryManager struct {
	// indexed by topic name
	recvdFailures                  map[string][]data.Failure
	willErrorOnPublishFailure      bool
	willErrorOnGetMessagesForRetry bool
	retryErrored                   bool
	retrySuccessful                bool
}

// GetBatch will return in-memory received failures as retries
func (mr *mockRetryManager) GetBatch(ctx context.Context, topic string, sequence uint8, interval time.Duration) ([]model.Retry, error) {
	if mr.willErrorOnGetMessagesForRetry {
		return nil, errors.New("oops")
	}

	failures, ok := mr.recvdFailures[topic]
	if !ok {
		return []model.Retry{}, nil
	}

	var rts []model.Retry
	for _, failure := range failures {
		rts = append(rts, model.Retry{
			PayloadJSON:    failure.Message,
			PayloadHeaders: failure.MessageHeaders,
			PayloadKey:     failure.MessageKey,
			Topic:          failure.Topic,
			KafkaPartition: failure.KafkaPartition,
			KafkaOffset:    failure.KafkaOffset,
		})
	}

	return rts, nil
}

func (mr *mockRetryManager) MarkSuccessful(ctx context.Context, retry model.Retry) error {
	mr.retrySuccessful = true
	return nil
}

func (mr *mockRetryManager) MarkErrored(ctx context.Context, retry model.Retry, err error) error {
	mr.retryErrored = true
	return nil
}

func (mr *mockRetryManager) PublishFailure(ctx context.Context, f data.Failure) error {
	if mr.willErrorOnPublishFailure {
		return errors.New("oops")
	}
	mr.recvdFailures[f.Topic] = append(mr.recvdFailures[f.Topic], f)
	return nil
}

func newMockRetryManager(willError bool) *mockRetryManager {
	return &mockRetryManager{
		recvdFailures:             map[string][]data.Failure{},
		willErrorOnPublishFailure: willError,
	}
}

func (mr *mockRetryManager) getPublishedFailureCountByTopic(topic string) int {
	f, ok := mr.recvdFailures[topic]
	if !ok {
		return 0
	}
	return len(f)
}

func (mr *mockRetryManager) getFirstPublishedFailureByTopic(topic string) *data.Failure {
	f, ok := mr.recvdFailures[topic]
	if !ok {
		return nil
	}
	return &f[0]
}
