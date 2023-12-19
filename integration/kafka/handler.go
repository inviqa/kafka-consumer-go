//go:build integration
// +build integration

package kafka

import (
	"context"
	"errors"
	"log"

	"github.com/IBM/sarama"
)

type TestConsumerHandler struct {
	RecvdMessages       []*sarama.ConsumerMessage
	fail                bool
	failOnMessageNumber *int
}

func NewTestConsumerHandler() *TestConsumerHandler {
	return &TestConsumerHandler{}
}

func (t *TestConsumerHandler) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	log.Printf("handling message with topic: %s, offset: %d, partition: %d\n", msg.Topic, msg.Offset, msg.Partition)

	t.RecvdMessages = append(t.RecvdMessages, msg)

	if t.fail {
		return errors.New("oops")
	}

	if t.failOnMessageNumber != nil && len(t.RecvdMessages) == (*t.failOnMessageNumber) {
		return errors.New("oops")
	}

	return nil
}

func (t *TestConsumerHandler) WillFail() {
	t.fail = true
}

func (t *TestConsumerHandler) WillFailOn(messageNumber int) {
	t.failOnMessageNumber = &messageNumber
}
