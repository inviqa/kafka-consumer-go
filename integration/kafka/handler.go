// +build integration

package kafka

import (
	"errors"

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
	t.RecvdMessages = append(t.RecvdMessages, msg)

	if t.fail {
		return errors.New("oops")
	}

	return nil
}

func (t *TestConsumerHandler) WillFail() {
	t.fail = true
}
