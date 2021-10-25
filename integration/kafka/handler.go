// +build integration

package kafka

import (
	"errors"
	"log"

	"github.com/Shopify/sarama"
)

type TestConsumerHandler struct {
	RecvdMessages []*sarama.ConsumerMessage
	fail          bool
}

func NewTestConsumerHandler() *TestConsumerHandler {
	return &TestConsumerHandler{}
}

func (t *TestConsumerHandler) Handle(msg *sarama.ConsumerMessage) error {
	log.Printf("handling message with topic: %s, offset: %d, partition: %d\n", msg.Topic, msg.Offset, msg.Partition)

	t.RecvdMessages = append(t.RecvdMessages, msg)

	if t.fail {
		return errors.New("oops")
	}

	return nil
}

func (t *TestConsumerHandler) WillFail() {
	t.fail = true
}
