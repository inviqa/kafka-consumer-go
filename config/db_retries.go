package config

import (
	"time"

	"github.com/inviqa/kafka-consumer-go/data/retry/model"
)

type DBRetries map[string][]*DBTopicRetry

// DBTopicRetry represents retry configuration for a topic when it is processed from the DB.
type DBTopicRetry struct {
	// Interval represents the minimum time that should elapse between the last processing attempt and the next one.
	Interval time.Duration
	// Sequence represents the sequence number of this retry attempt. This is used to find retries that are at the right
	// sequence in the retry flow.
	Sequence uint8
	Key      TopicKey
}

// MakeRetryErrored will increment the Attempts field on the retry, and then mark it errored
// and, if no more attempts are needed, deadlettered also. A new copy of the retry will be
// returned so callers should use this instead of the original retry passed to the func.
func (dr DBRetries) MakeRetryErrored(retry model.Retry) model.Retry {
	retry.Errored = true
	retry.Attempts = retry.Attempts + 1

	if retry.Attempts >= dr.maxAttemptsForTopic(retry.Topic) {
		retry.Deadlettered = true
	}

	return retry
}

// MakeRetrySuccessful will increment the Attempts field on the retry, and then remove the
// errored flag. A new copy of the retry will be returned so callers should use this
// instead of the original retry passed to the func.
func (dr DBRetries) MakeRetrySuccessful(retry model.Retry) model.Retry {
	retry.Attempts = retry.Attempts + 1
	retry.Errored = false
	return retry
}

func (dr DBRetries) maxAttemptsForTopic(topic string) uint8 {
	retries, ok := dr[topic]
	if !ok {
		return 0
	}
	last := retries[len(retries)-1]
	return last.Sequence
}
