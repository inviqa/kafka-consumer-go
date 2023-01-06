package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/log"
)

// kafkaFailureProducer is a producer that listens for failed push attempts from kafka
// on fch and then sends them to the next kafka retry topic in the chain for retry later
type kafkaFailureProducer struct {
	producer sarama.SyncProducer
	fch      <-chan model.Failure
	logger   log.Logger
}

func newKafkaFailureProducerWithDefaults(cfg *config.Config, fch <-chan model.Failure, logger log.Logger) (*kafkaFailureProducer, error) {
	if logger == nil {
		logger = log.NullLogger{}
	}

	var sp sarama.SyncProducer
	var err error

	for i := 0; i < maxConnectionAttempts; i++ {
		sp, err = sarama.NewSyncProducer(cfg.Host, config.NewSaramaConfig(cfg.TLSEnable, cfg.TLSSkipVerifyPeer))
		if err == nil {
			break
		}

		// the cluster may be temporarily unreachable so if we see ErrOutOfBrokers we continue to the
		// next iteration to make another attempt to connect (the sarama.NewSyncProducer also internally
		// makes retry attempts so we should connect within 3 attempts here (see maxConnectionAttempts)
		if !errors.Is(err, sarama.ErrOutOfBrokers) {
			return nil, fmt.Errorf("error occurred creating Kafka producer for retries: %w", err)
		}

		logger.Info("Kafka cluster is not reachable, retrying...")
		time.Sleep(connectionInterval)
	}

	return newKafkaFailureProducer(sp, fch, logger), nil
}

func newKafkaFailureProducer(sp sarama.SyncProducer, fch <-chan model.Failure, logger log.Logger) *kafkaFailureProducer {
	return &kafkaFailureProducer{
		producer: sp,
		fch:      fch,
		logger:   logger,
	}
}

func (p kafkaFailureProducer) listenForFailures(ctx context.Context, wg *sync.WaitGroup) {
	p.logger.Info("starting Kafka retry producer")

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if err := p.producer.Close(); err != nil {
				p.logger.Error("error occurred closing Kafka retry producer")
			}
		}()

		for {
			select {
			case f := <-p.fch:
				p.publishFailure(f)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (p kafkaFailureProducer) publishFailure(f model.Failure) {
	p.logger.Debugf("publishing retry to Kafka topic '%s'", f.NextTopic)

	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: f.NextTopic,
		Value: sarama.ByteEncoder(f.Message),
	})

	if err != nil {
		p.logger.Errorf("error occurred publishing retry to Kafka topic '%s': %w", f.NextTopic, err)
		return
	}

	p.logger.Debugf("published Failure event message to Kafka retry topic '%s' successfully", f.NextTopic)
}
