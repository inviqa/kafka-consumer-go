package saramatest

import (
	"context"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
)

type MockConsumerGroup struct {
	// consumed will mark the consumer group as having been consumed, which makes
	// our tests easier to implement as we do not need to cater for an ongoing-consume
	// as under normal operation
	consumed bool
	closed             bool
	errorOnClose       bool
	consumedTopicCount map[string]int
	errorOnConsume     bool
	// indexed by topic name
	msgsToConsume map[string][]*sarama.ConsumerMessage
	// errors from the consume claim on the topic, indexed by topic
	consumeClaimErrs map[string]error
	sync.RWMutex
}

func NewMockConsumerGroup() *MockConsumerGroup {
	return &MockConsumerGroup{
		closed:             false,
		errorOnClose:       false,
		consumedTopicCount: map[string]int{},
		msgsToConsume:      map[string][]*sarama.ConsumerMessage{},
		consumeClaimErrs:   map[string]error{},
	}
}

func (mg *MockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	mg.Lock()
	defer mg.Unlock()

	if mg.consumed {
		return nil
	}
	mg.consumed = true

	if mg.errorOnConsume {
		return errors.New("something bad happened")
	}

	for _, topic := range topics {
		_, ok := mg.consumedTopicCount[topic]
		if !ok {
			mg.consumedTopicCount[topic] = 0
		}
		mg.consumedTopicCount[topic]++

		msgsToConsume := mg.messagesToConsumeForTopic(topic)
		if len(msgsToConsume) == 0 {
			continue
		}
		session := NewMockConsumerGroupSession()
		claim := NewMockConsumerGroupClaim()
		for _, msg := range msgsToConsume {
			claim.PublishMessage(msg)
		}
		claim.CloseChannel()
		if err := handler.ConsumeClaim(session, claim); err != nil {
			mg.consumeClaimErrs[topic] = err
		}
	}

	return nil
}

func (mg *MockConsumerGroup) Errors() <-chan error {
	return make(<-chan error, 100)
}

func (mg *MockConsumerGroup) Close() error {
	if mg.errorOnClose {
		return errors.New("something bad happened")
	}

	mg.closed = true

	return nil
}

func (mg *MockConsumerGroup) WasClosed() bool {
	return mg.closed
}

func (mg *MockConsumerGroup) ErrorOnClose() {
	mg.errorOnClose = true
}

func (mg *MockConsumerGroup) GetTopicConsumeCount(t string) int {
	mg.RLock()
	defer mg.RUnlock()

	return mg.consumedTopicCount[t]
}

func (mg *MockConsumerGroup) ErrorOnConsume() {
	mg.errorOnConsume = true
}

func (mg *MockConsumerGroup) AddMessage(msg *sarama.ConsumerMessage) {
	mg.msgsToConsume[msg.Topic] = append(mg.msgsToConsume[msg.Topic], msg)
}

func (mg *MockConsumerGroup) messagesToConsumeForTopic(topic string) []*sarama.ConsumerMessage {
	msgs, ok := mg.msgsToConsume[topic]
	if !ok {
		return []*sarama.ConsumerMessage{}
	}
	return msgs
}
