package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/log"
)

type collection interface {
	start(ctx context.Context, wg *sync.WaitGroup) error
	close()
}

func connectToKafka(cfg *config.Config, saramaCfg *sarama.Config, logger log.Logger) (sarama.ConsumerGroup, error) {
	var cl sarama.ConsumerGroup
	var err error

	for i := 0; i < maxConnectionAttempts; i++ {
		cl, err = sarama.NewConsumerGroup(cfg.Host, cfg.Group, saramaCfg)
		if err == nil {
			break
		}

		// the cluster may be temporarily unreachable so if we see ErrOutOfBrokers we continue to the
		// next iteration to make another attempt to connect
		if !errors.Is(err, sarama.ErrOutOfBrokers) {
			return nil, fmt.Errorf("error occurred creating Kafka consumer group client: %w", err)
		}

		logger.Info("Kafka cluster is not reachable, retrying...")
		time.Sleep(connectionInterval)
	}

	if cl == nil {
		return nil, errors.New("unable to connect to Kafka cluster, maybe it is unreachable or not ready yet")
	}

	return cl, nil
}
