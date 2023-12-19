package consumer

import (
	"github.com/IBM/sarama"

	"github.com/revdaalex/kafka-consumer-go/config"
	"github.com/revdaalex/kafka-consumer-go/log"
)

type kafkaConnector func(cfg *config.Config, saramaCfg *sarama.Config, logger log.Logger) (sarama.ConsumerGroup, error)
