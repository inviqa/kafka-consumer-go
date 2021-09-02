// +build integration

package integration

import (
	"testing"

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
			if len(handler.RecvdMessages) == 2 {
				doneCh <- true
				return
			}
		}
	}, handler.Handle)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(handler.RecvdMessages) != 2 {
		t.Errorf("expected 2 messages to be received by handler, received %d", len(handler.RecvdMessages))
	}
}
