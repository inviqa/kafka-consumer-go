package consumer

import (
	"errors"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/log"
	"github.com/inviqa/kafka-consumer-go/test/saramatest"
)

type testKafkaConnector struct {
	consumerGroup sarama.ConsumerGroup
	willError     bool
}

// connectToKafka satisfies the kafkaConnector type and is used from tests
func (t testKafkaConnector) connectToKafka(cfg *config.Config, saramaCfg *sarama.Config, logger log.Logger) (sarama.ConsumerGroup, error) {
	if t.willError {
		return nil, errors.New("oops")
	}

	if t.consumerGroup == nil {
		return saramatest.NewMockConsumerGroup(), nil
	}

	return t.consumerGroup, nil
}
