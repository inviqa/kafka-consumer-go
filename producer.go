package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/inviqa/kafka-consumer-go/config"
)

type FailureProducer interface {
	ListenForFailures(ctx context.Context, wg *sync.WaitGroup)
}

type failureProducer struct {
	producer sarama.SyncProducer
	fch      <-chan Failure
	logger   Logger
}

func NewFailureProducerWithDefaults(cfg *config.Config, fch <-chan Failure, log Logger) (FailureProducer, error) {
	var sp sarama.SyncProducer
	var err error

	if log == nil {
		log = NullLogger{}
	}

	for i := 0; i < maxConnectionAttempts; i++ {
		sp, err = sarama.NewSyncProducer(cfg.Host, config.NewSaramaConfig(cfg.TLSEnable, cfg.TLSSkipVerifyPeer))
		if err == nil {
			break
		}

		// the cluster may be temporarily unreachable so if we see ErrOutOfBrokers we continue to the
		// next iteration to make another attempt to connect (the sarama.NewSyncProducer also internally
		// makes retry attempts so we should connect within 3 attempts here (see maxConnectionAttempts)
		if err != sarama.ErrOutOfBrokers {
			return nil, fmt.Errorf("error occurred creating Kafka producer for retries: %w", err)
		}

		log.Info("Kafka cluster is not reachable, retrying...")
		time.Sleep(connectionInterval)
	}

	return NewFailureProducer(sp, fch, log), nil
}

func NewFailureProducer(p sarama.SyncProducer, fch <-chan Failure, log Logger) FailureProducer {
	if log == nil {
		log = NullLogger{}
	}

	return &failureProducer{
		producer: p,
		fch:      fch,
		logger:   log,
	}
}

func (p failureProducer) ListenForFailures(ctx context.Context, wg *sync.WaitGroup) {
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

func (p failureProducer) publishFailure(f Failure) {
	p.logger.Debugf("publishing retry to Kafka topic '%s'", f.TopicToSendTo)

	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: f.TopicToSendTo,
		Value: sarama.ByteEncoder(f.Message),
	})

	if err != nil {
		p.logger.Errorf("error occurred publishing retry to Kafka topic '%s': %w", f.TopicToSendTo, err)
		return
	}

	p.logger.Debugf("published Failure event message to Kafka retry topic '%s' successfully", f.TopicToSendTo)
}
