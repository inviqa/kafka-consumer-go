package saramatest

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
)

type MockConsumerGroupSession struct {
	sync.RWMutex
	marked []*sarama.ConsumerMessage
	ctx    context.Context
}

func NewMockConsumerGroupSession() *MockConsumerGroupSession {
	return &MockConsumerGroupSession{
		ctx: context.Background(),
	}
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
	gs.Lock()
	defer gs.Unlock()
	gs.marked = append(gs.marked, msg)
}

func (gs *MockConsumerGroupSession) MessageWasMarked(msg *sarama.ConsumerMessage) bool {
	gs.RLock()
	defer gs.RUnlock()

	for _, m := range gs.marked {
		if m == msg {
			return true
		}
	}

	return false
}

func (gs *MockConsumerGroupSession) Context() context.Context {
	gs.RLock()
	defer gs.RUnlock()
	return gs.ctx
}

func (gs *MockConsumerGroupSession) SetContext(ctx context.Context) {
	gs.Lock()
	defer gs.Unlock()
	gs.ctx = ctx
}
