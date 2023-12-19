package model

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/go-test/deep"
)

func TestRetry_ToSaramaConsumerMessage(t *testing.T) {
	retry := Retry{
		ID:             10,
		Topic:          "product",
		PayloadJSON:    []byte(`{"foo":"bar"}`),
		PayloadHeaders: []byte(`{"baz":"buzz"}`),
		PayloadKey:     []byte("foo"),
		KafkaOffset:    100,
		KafkaPartition: 101,
		Attempts:       1,
	}

	t.Run("retry converts to consumer message", func(t *testing.T) {
		t.Parallel()
		exp := &sarama.ConsumerMessage{
			Headers:   []*sarama.RecordHeader{{Key: []byte("baz"), Value: []byte("buzz")}},
			Key:       []byte("foo"),
			Value:     []byte(`{"foo":"bar"}`),
			Topic:     "product",
			Partition: 101,
			Offset:    100,
		}

		got := retry.ToSaramaConsumerMessage()
		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("retry with nil headers converts to consumer message", func(t *testing.T) {
		t.Parallel()
		retry2 := retry
		retry2.PayloadHeaders = nil
		exp := &sarama.ConsumerMessage{
			Key:       []byte("foo"),
			Value:     []byte(`{"foo":"bar"}`),
			Topic:     "product",
			Partition: 101,
			Offset:    100,
		}

		got := retry2.ToSaramaConsumerMessage()
		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("retry with empty headers converts to consumer message", func(t *testing.T) {
		t.Parallel()
		retry2 := retry
		retry2.PayloadHeaders = []byte(`{}`)
		exp := &sarama.ConsumerMessage{
			Key:       []byte("foo"),
			Value:     []byte(`{"foo":"bar"}`),
			Topic:     "product",
			Partition: 101,
			Offset:    100,
		}

		got := retry2.ToSaramaConsumerMessage()
		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})
}
