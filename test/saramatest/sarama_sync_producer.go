package saramatest

import (
	"errors"

	"github.com/Shopify/sarama"
)

type MockSyncProducer struct {
	recvd       map[string][][]byte
	returnError bool
}

func NewMockSyncProducer() *MockSyncProducer {
	return &MockSyncProducer{
		recvd: map[string][][]byte{},
	}
}

func (p *MockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if p.returnError {
		return 0, 0, errors.New("oops, send errored")
	}

	b, err := msg.Value.Encode()
	if err != nil {
		panic(err)
	}

	p.recvd[msg.Topic] = append(p.recvd[msg.Topic], b)

	return 0, 0, nil
}

func (p *MockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return nil
}

func (p *MockSyncProducer) Close() error {
	return nil
}

func (p *MockSyncProducer) ReturnErrorOnSend() {
	p.returnError = true
}

func (p *MockSyncProducer) GetLastMessageReceived(topic string) []byte {
	if len(p.recvd[topic]) == 0 {
		return []byte(``)
	}

	return p.recvd[topic][0]
}
