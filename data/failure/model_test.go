package failure

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/go-test/deep"
)

func TestFailureFromSaramaMessage(t *testing.T) {
	exampleMsg := &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{{
			Key:   []byte("foo"),
			Value: []byte("buzz"),
		}},
		Key:       []byte("baz"),
		Value:     []byte(`{"foo":"bar"}`),
		Topic:     "product",
		Partition: 21002,
		Offset:    3048453957483304,
	}

	t.Run("failure created from sarama message", func(t *testing.T) {
		exp := Failure{
			Reason:         "something bad happened",
			Topic:          "product",
			NextTopic:      "retry1.product",
			Message:        []byte(`{"foo":"bar"}`),
			MessageKey:     []byte("baz"),
			MessageHeaders: []byte(`{"foo":"buzz"}`),
			KafkaPartition: 21002,
			KafkaOffset:    3048453957483304,
		}

		err := errors.New("something bad happened")
		got := FailureFromSaramaMessage(err, "retry1.product", exampleMsg)
		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})
}
