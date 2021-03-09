package test

import "github.com/Shopify/sarama"

type MockConsumerGroupHandler struct{}

func (handler MockConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (handler MockConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (handler MockConsumerGroupHandler) ConsumeClaim(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error {
	return nil
}

func NewMockConsumerGroupHandler() *MockConsumerGroupHandler {
	return &MockConsumerGroupHandler{}
}
