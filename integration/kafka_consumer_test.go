//go:build integration
// +build integration

package integration

import (
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/data/retry/model"
	"github.com/inviqa/kafka-consumer-go/integration/kafka"
)

func TestMessagesAreConsumedFromKafka(t *testing.T) {
	publishTestMessageToKafka(kafka.TestMessage{})

	handler := kafka.NewTestConsumerHandler()

	err := consumeFromKafkaUntil(func(doneCh chan<- bool) {
		for {
			if len(handler.RecvdMessages) == 1 {
				doneCh <- true
				return
			}
		}
	}, handler.Handle)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(handler.RecvdMessages) != 1 {
		t.Errorf("expected 1 message to be received by handler, received %d", len(handler.RecvdMessages))
	}
}

func TestMessagesAreConsumedFromKafka_WithError(t *testing.T) {
	publishTestMessageToKafka(kafka.TestMessage{})

	handler := kafka.NewTestConsumerHandler()
	handler.WillFail()

	err := consumeFromKafkaUntil(func(doneCh chan<- bool) {
		for {
			if len(handler.RecvdMessages) == 3 {
				doneCh <- true
				return
			}
			time.Sleep(time.Millisecond * 500)
		}
	}, handler.Handle)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(handler.RecvdMessages) != 3 {
		t.Errorf("expected 3 messages to be received by handler, received %d", len(handler.RecvdMessages))
	}
}

func TestMessagesAreConsumedFromKafka_WithDbRetries(t *testing.T) {
	t.Run("it marks retries as successful if they succeed on retry", func(t *testing.T) {
		publishTestMessageToKafka(kafka.TestMessage{
			XEventId: "test-consume-db-retry-1",
		})

		handler := kafka.NewTestConsumerHandler()
		handler.WillFailOn(1)

		err := consumeFromKafkaUsingDbRetriesUntil(func(doneCh chan<- bool) {
			for {
				if len(handler.RecvdMessages) >= 2 {
					doneCh <- true
					return
				}
			}
		}, handler.Handle)

		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		// we expect 2 messages to have been received, one for the original consume operation and
		// another for retry which should have been picked up from the database retry table
		if len(handler.RecvdMessages) != 2 {
			t.Fatalf("expected 2 messages to be received by handler, received %d", len(handler.RecvdMessages))
		}

		got, err := dbRetryWithEventId("test-consume-db-retry-1")
		if err != nil {
			t.Fatal(err)
		}

		exp := testExpectedRetryModelBase("test-consume-db-retry-1", got.ID, got.KafkaOffset)
		exp.Successful = true
		exp.Attempts = 2

		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("it marks retries as deadlettered if they fail on all retries", func(t *testing.T) {
		publishTestMessageToKafka(kafka.TestMessage{
			XEventId: "test-consume-db-retry-2",
		})

		handler := kafka.NewTestConsumerHandler()
		handler.WillFail()

		err := consumeFromKafkaUsingDbRetriesUntil(func(doneCh chan<- bool) {
			for {
				if len(handler.RecvdMessages) >= 3 {
					doneCh <- true
					return
				}
			}
		}, handler.Handle)

		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		// we expect 3 messages to have been received, one for the original consume operation and
		// another two for retries which should have been picked up from the database retry table
		if len(handler.RecvdMessages) != 3 {
			t.Fatalf("expected 3 messages to be received by handler, received %d", len(handler.RecvdMessages))
		}

		got, err := dbRetryWithEventId("test-consume-db-retry-2")
		if err != nil {
			t.Fatal(err)
		}

		exp := testExpectedRetryModelBase("test-consume-db-retry-2", got.ID, got.KafkaOffset)
		exp.Deadlettered = true
		exp.Errored = true
		exp.LastError = "oops"

		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})
}

func testExpectedRetryModelBase(eventId string, id, offset int64) *Retry {
	return &Retry{
		Retry: model.Retry{
			ID:             id,
			Topic:          "mainTopic",
			PayloadJSON:    []byte(`{"type":"","data":{},"event_id":"` + eventId + `"}`),
			PayloadHeaders: []byte(`{"foo":"bar"}`),
			PayloadKey:     []byte(`message-key`),
			KafkaOffset:    offset,
			KafkaPartition: 0,
			Attempts:       3,
		},
	}
}
