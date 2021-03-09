package test

import (
	"context"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
)

type MockConsumerGroup struct {
	closed             bool
	errorOnClose       bool
	consumedTopicCount map[string]int
	errorOnConsume     bool
	sync.RWMutex
}

func NewMockConsumerGroup() *MockConsumerGroup {
	return &MockConsumerGroup{
		closed:             false,
		errorOnClose:       false,
		consumedTopicCount: map[string]int{},
	}
}

func (mg *MockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	mg.Lock()
	defer mg.Unlock()

	if mg.errorOnConsume {
		return errors.New("something bad happened")
	}

	for _, n := range topics {
		_, ok := mg.consumedTopicCount[n]
		if !ok {
			mg.consumedTopicCount[n] = 0
		}
		mg.consumedTopicCount[n]++
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
