package test

import "github.com/Shopify/sarama"

type MockConsumerGroupClaim struct {
	Chan chan *sarama.ConsumerMessage
}

func NewMockConsumerGroupClaim() *MockConsumerGroupClaim {
	return &MockConsumerGroupClaim{
		Chan: make(chan *sarama.ConsumerMessage, 10),
	}
}

func (gc MockConsumerGroupClaim) Topic() string {
	return ""
}

func (gc MockConsumerGroupClaim) Partition() int32 {
	return 1
}

func (gc MockConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

func (gc MockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}

func (gc MockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return gc.Chan
}

func (gc MockConsumerGroupClaim) PublishMessage(m *sarama.ConsumerMessage) {
	gc.Chan <- m
}

func (gc MockConsumerGroupClaim) CloseChannel() {
	close(gc.Chan)
}
