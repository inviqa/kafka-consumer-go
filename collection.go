package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/inviqa/kafka-consumer-go/config"
)

type Collection struct {
	cfg       *config.Config
	consumers []sarama.ConsumerGroup
	producer  FailureProducer
	handler   sarama.ConsumerGroupHandler
	saramaCfg *sarama.Config
	logger    Logger
}

func NewCollection(cfg *config.Config, p FailureProducer, handler sarama.ConsumerGroupHandler, scfg *sarama.Config, log Logger) *Collection {
	if log == nil {
		log = nullLogger{}
	}

	return &Collection{
		cfg:       cfg,
		consumers: []sarama.ConsumerGroup{},
		producer:  p,
		handler:   handler,
		saramaCfg: scfg,
		logger:    log,
	}
}

func (cc *Collection) Start(ctx context.Context, wg *sync.WaitGroup) error {
	topics := cc.cfg.ConsumableTopics
	if topics == nil {
		return errors.New("no Kafka topics are configured, therefore cannot start consumers")
	}

	for _, t := range topics {
		group, err := cc.startConsumerGroup(ctx, wg, t)
		if err != nil {
			return err
		}
		cc.consumers = append(cc.consumers, group)
	}
	cc.producer.ListenForFailures(wg)

	return nil
}

func (cc *Collection) Close() {
	for _, c := range cc.consumers {
		if err := c.Close(); err != nil {
			cc.logger.Errorf("error occurred closing a Kafka consumer: %w", err)
		}
	}
	cc.consumers = []sarama.ConsumerGroup{}
}

func (cc *Collection) startConsumerGroup(ctx context.Context, wg *sync.WaitGroup, topic *config.KafkaTopic) (sarama.ConsumerGroup, error) {
	cc.logger.Infof("starting Kafka consumer group for '%s'", topic.Name)

	var cl sarama.ConsumerGroup
	var err error
	for i := 0; i < maxConnectionAttempts; i++ {
		cl, err = sarama.NewConsumerGroup(cc.cfg.Host, cc.cfg.Group, cc.saramaCfg)
		if err == nil {
			break
		}

		// the cluster may be temporarily unreachable so if we see ErrOutOfBrokers we continue to the
		// next iteration to make another attempt to connect
		if err != sarama.ErrOutOfBrokers {
			return nil, fmt.Errorf("error occurred creating Kafka consumer group client: %w", err)
		}

		cc.logger.Info("Kafka cluster is not reachable, retrying...")
		time.Sleep(connectionInterval)
	}

	cc.startConsumer(cl, ctx, wg, topic)

	return cl, nil
}

func (cc *Collection) startConsumer(cl sarama.ConsumerGroup, ctx context.Context, wg *sync.WaitGroup, topic *config.KafkaTopic) {
	go func() {
		for err := range cl.Errors() {
			cc.logger.Errorf("error occurred in consumer group handler: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			timer := time.NewTimer(topic.Delay)
			select {
			case <-timer.C:
				if err := cl.Consume(ctx, []string{topic.Name}, cc.handler); err != nil {
					cc.logger.Errorf("error when consuming from Kafka", err)
				}
				if ctx.Err() != nil {
					return
				}
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}()
}
