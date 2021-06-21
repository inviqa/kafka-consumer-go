package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/inviqa/kafka-consumer-go/test"

	"github.com/go-test/deep"

	"github.com/Shopify/sarama"
	"github.com/inviqa/kafka-consumer-go/config"
)

func TestNewConsumer(t *testing.T) {
	deep.CompareUnexportedFields = true
	deep.MaxDepth = 2
	defer func() {
		deep.CompareUnexportedFields = false
		deep.MaxDepth = 0
	}()

	fch := make(chan Failure)
	cfg := &config.Config{}
	hs := HandlerMap{
		"product": func(msg *sarama.ConsumerMessage) error { return nil },
	}
	l := NullLogger{}

	exp := &consumer{
		failureCh: fch,
		cfg:       cfg,
		handlers:  hs,
		logger:    l,
	}

	if diff := deep.Equal(exp, NewConsumer(fch, cfg, hs, l)); diff != nil {
		t.Error(diff)
	}
}

func TestConsumer_ConsumeClaim(t *testing.T) {
	fch := make(chan Failure)
	cfg := newTestConfig()
	handler := &mockConsumerHandler{}
	hs := HandlerMap{
		"product": handler.handle,
	}
	l := NullLogger{}

	con := NewConsumer(fch, cfg, hs, l)

	gs := test.NewMockConsumerGroupSession()
	gc := test.NewMockConsumerGroupClaim()

	msg1 := &sarama.ConsumerMessage{Value: []byte(`{"type":"productCreated"}`), Topic: "product"}
	gc.PublishMessage(msg1)
	gc.CloseChannel()

	err := con.ConsumeClaim(gs, gc)
	if err != nil {
		t.Fatalf("unexpected error occurred: %s", err)
	}

	if !gs.MessageWasMarked(msg1) {
		t.Error("msg1 was not marked as processed")
	}

	if len(handler.recvdMessages) == 0 {
		t.Fatal("Handler did not receive any message")
	}

	if handler.recvdMessages[0] != msg1 {
		t.Errorf("Handler was expecting msg '%v', got '%v'", msg1, handler.recvdMessages[0])
	}
}

func TestConsumer_ConsumeClaim_WithFailure(t *testing.T) {
	fch := make(chan Failure, 1)
	cfg := newTestConfig()
	handler := &mockConsumerHandler{}
	handler.willFail()
	hs := HandlerMap{
		"product": handler.handle,
	}
	l := NullLogger{}

	con := NewConsumer(fch, cfg, hs, l)

	gs := test.NewMockConsumerGroupSession()
	gc := test.NewMockConsumerGroupClaim()

	msg1 := &sarama.ConsumerMessage{Value: []byte(`{"type":"productCreated"}`), Topic: "product"}
	gc.PublishMessage(msg1)
	gc.CloseChannel()

	err := con.ConsumeClaim(gs, gc)
	if err != nil {
		t.Fatalf("unexpected error occurred: %s", err)
	}

	if !gs.MessageWasMarked(msg1) {
		t.Error("msg1 was not marked as processed")
	}

	if len(handler.recvdMessages) == 0 {
		t.Fatal("Handler did not receive any message")
	}

	if handler.recvdMessages[0] != msg1 {
		t.Errorf("Handler was expecting msg '%v', got '%v'", msg1, handler.recvdMessages[0])
	}

	failureCount := 0
	for {
		select {
		case <-time.After(time.Millisecond * 100):
			if failureCount != 1 {
				t.Errorf("expected %d messages in failure channel, got %d", 1, failureCount)
			}
			return
		case <-fch:
			failureCount++
		}
	}
}

func newTestConfig() *config.Config {
	deadLetterProduct := &config.KafkaTopic{
		Name: "deadLetter.kafkaGroup.product",
		Key:  "product",
	}
	retryProduct := &config.KafkaTopic{
		Name:  "retry.kafkaGroup.product",
		Delay: 1,
		Key:   "product",
		Next:  deadLetterProduct,
	}
	product := &config.KafkaTopic{
		Name: "product",
		Key:  "product",
		Next: retryProduct,
	}

	return &config.Config{
		Group: "kafkaGroup",
		TopicMap: map[config.TopicKey]*config.KafkaTopic{
			"product":                       product,
			"retry.kafkaGroup.product":      retryProduct,
			"deadLetter.kafkaGroup.product": deadLetterProduct,
		},
	}
}

type mockConsumerHandler struct {
	recvdMessages []*sarama.ConsumerMessage
	fail          bool
}

func (m *mockConsumerHandler) handle(msg *sarama.ConsumerMessage) error {
	m.recvdMessages = append(m.recvdMessages, msg)

	if m.fail {
		return errors.New("oops")
	}

	return nil
}

func (m *mockConsumerHandler) willFail() {
	m.fail = true
}
