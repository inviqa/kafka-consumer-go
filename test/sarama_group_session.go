package test

import (
	"context"

	"github.com/Shopify/sarama"
)

type MockConsumerGroupSession struct {
	marked []*sarama.ConsumerMessage
}

func NewMockConsumerGroupSession() *MockConsumerGroupSession {
	return &MockConsumerGroupSession{}
}

func (gs *MockConsumerGroupSession) Claims() map[string][]int32 {
	return map[string][]int32{}
}

func (gs *MockConsumerGroupSession) MemberID() string {
	return ""
}

func (gs *MockConsumerGroupSession) GenerationID() int32 {
	return 0
}

func (gs *MockConsumerGroupSession) Commit() {
}

func (gs *MockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}

func (gs *MockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}

func (gs *MockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	gs.marked = append(gs.marked, msg)
}

func (gs *MockConsumerGroupSession) MessageWasMarked(msg *sarama.ConsumerMessage) bool {
	for _, m := range gs.marked {
		if m == msg {
			return true
		}
	}

	return false
}

func (gs *MockConsumerGroupSession) Context() context.Context {
	return context.Background()
}
