package consumer

import (
	"errors"
	"time"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries"
)

type mockRetriesRepository struct {
	// indexed by topic name
	recvdFailures                  map[string][]data.Failure
	willErrorOnPublishFailure      bool
	willErrorOnGetMessagesForRetry bool
	retryErrored                   bool
	retrySuccessful                bool
}

// GetMessagesForRetry will return in-memory received failures as retries
func (mr *mockRetriesRepository) GetMessagesForRetry(topic string, sequence uint8, interval time.Duration) ([]retries.Retry, error) {
	if mr.willErrorOnGetMessagesForRetry {
		return nil, errors.New("oops")
	}

	failures, ok := mr.recvdFailures[topic]
	if !ok {
		return []retries.Retry{}, nil
	}

	var rts []retries.Retry
	for _, failure := range failures {
		rts = append(rts, retries.Retry{
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

func (mr *mockRetriesRepository) MarkRetrySuccessful(id int64) error {
	mr.retrySuccessful = true
	return nil
}

func (mr *mockRetriesRepository) MarkRetryErrored(retry retries.Retry) error {
	mr.retryErrored = true
	return nil
}

func newMockRetriesRepository(willError bool) *mockRetriesRepository {
	return &mockRetriesRepository{
		recvdFailures:             map[string][]data.Failure{},
		willErrorOnPublishFailure: willError,
	}
}

func (mr *mockRetriesRepository) PublishFailure(f data.Failure) error {
	if mr.willErrorOnPublishFailure {
		return errors.New("oops")
	}
	mr.recvdFailures[f.Topic] = append(mr.recvdFailures[f.Topic], f)
	return nil
}

func (mr *mockRetriesRepository) getPublishedFailureCountByTopic(topic string) int {
	f, ok := mr.recvdFailures[topic]
	if !ok {
		return 0
	}
	return len(f)
}

func (mr *mockRetriesRepository) getFirstPublishedFailureByTopic(topic string) *data.Failure {
	f, ok := mr.recvdFailures[topic]
	if !ok {
		return nil
	}
	return &f[0]
}
