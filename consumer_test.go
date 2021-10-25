package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/log"
	"github.com/inviqa/kafka-consumer-go/test/saramatest"
)

func TestNewConsumer(t *testing.T) {
	deep.CompareUnexportedFields = true
	deep.MaxDepth = 2
	defer func() {
		deep.CompareUnexportedFields = false
		deep.MaxDepth = 0
	}()

	fch := make(chan model.Failure)
	cfg := &config.Config{}
	hs := HandlerMap{
		"product": func(msg *sarama.ConsumerMessage) error { return nil },
	}
	l := log.NullLogger{}

	exp := &consumer{
		failureCh: fch,
		cfg:       cfg,
		handlers:  hs,
		logger:    l,
	}

	if diff := deep.Equal(exp, newConsumer(fch, cfg, hs, l)); diff != nil {
		t.Error(diff)
	}
}

func TestConsumer_ConsumeClaim(t *testing.T) {
	fch := make(chan model.Failure)
	cfg := newTestConfig()
	handler := &mockConsumerHandler{}
	hs := HandlerMap{
		"product": handler.handle,
	}
	l := log.NullLogger{}

	con := newConsumer(fch, cfg, hs, l)

	gs := saramatest.NewMockConsumerGroupSession()
	gc := saramatest.NewMockConsumerGroupClaim()

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
	fch := make(chan model.Failure, 1)
	cfg := newTestConfig()
	handler := &mockConsumerHandler{}
	handler.willFail()
	hs := HandlerMap{
		"product": handler.handle,
	}

	gs := saramatest.NewMockConsumerGroupSession()
	gc := saramatest.NewMockConsumerGroupClaim()

	msg1 := &sarama.ConsumerMessage{
		Value:     []byte(`{"type":"productCreated"}`),
		Topic:     "product",
		Offset:    10001,
		Partition: 2,
		Key:       []byte("SKU-123"),
	}
	gc.PublishMessage(msg1)
	gc.CloseChannel()

	con := newConsumer(fch, cfg, hs, log.NullLogger{})
	if err := con.ConsumeClaim(gs, gc); err != nil {
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
		case got := <-fch:
			exp := model.Failure{
				Reason:         "oops",
				Topic:          "product",
				NextTopic:      "retry.kafkaGroup.product",
				MessageHeaders: []byte(`{}`),
				Message:        []byte(`{"type":"productCreated"}`),
				MessageKey:     []byte("SKU-123"),
				KafkaOffset:    10001,
				KafkaPartition: 2,
			}
			if diff := deep.Equal(exp, got); diff != nil {
				t.Error(diff)
			}
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
		ConsumableTopics: []*config.KafkaTopic{product, retryProduct},
		DBRetries: map[string][]*config.DBTopicRetry{
			"product": {
				{
					Interval: time.Millisecond * 10,
					Sequence: 1,
					Key:      "product",
				},
			},
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
