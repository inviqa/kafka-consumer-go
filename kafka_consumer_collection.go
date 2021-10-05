package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data/failure"
	"github.com/inviqa/kafka-consumer-go/log"
)

type kafkaConsumerCollection struct {
	cfg       *config.Config
	consumers []sarama.ConsumerGroup
	producer  *kafkaFailureProducer
	handler   sarama.ConsumerGroupHandler
	saramaCfg *sarama.Config
	logger    log.Logger
}

func newKafkaConsumerCollection(
	cfg *config.Config,
	p *kafkaFailureProducer,
	fch chan failure.Failure,
	hm HandlerMap,
	scfg *sarama.Config,
	logger log.Logger,
) *kafkaConsumerCollection {
	if logger == nil {
		logger = log.NullLogger{}
	}

	return &kafkaConsumerCollection{
		cfg:       cfg,
		consumers: []sarama.ConsumerGroup{},
		producer:  p,
		handler:   NewConsumer(fch, cfg, hm, logger),
		saramaCfg: scfg,
		logger:    logger,
	}
}

func (cc *kafkaConsumerCollection) Start(ctx context.Context, wg *sync.WaitGroup) error {
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
	cc.producer.listenForFailures(ctx, wg)

	return nil
}

func (cc *kafkaConsumerCollection) Close() {
	for _, c := range cc.consumers {
		if err := c.Close(); err != nil {
			cc.logger.Errorf("error occurred closing a Kafka consumer: %w", err)
		}
	}
	cc.consumers = []sarama.ConsumerGroup{}
}

func (cc *kafkaConsumerCollection) startConsumerGroup(ctx context.Context, wg *sync.WaitGroup, topic *config.KafkaTopic) (sarama.ConsumerGroup, error) {
	cc.logger.Infof("starting Kafka consumer group for '%s'", topic.Name)

	cl, err := connectToKafka(cc.cfg, cc.saramaCfg, cc.logger)
	if err != nil {
		return nil, err
	}

	cc.startConsumer(cl, ctx, wg, topic)

	return cl, nil
}

func (cc *kafkaConsumerCollection) startConsumer(cl sarama.ConsumerGroup, ctx context.Context, wg *sync.WaitGroup, topic *config.KafkaTopic) {
	go func() {
		for err := range cl.Errors() {
			cc.logger.Errorf("error occurred in consumer group Handler: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(topic.Delay)
		for {
			select {
			case <-timer.C:
				if err := cl.Consume(ctx, []string{topic.Name}, cc.handler); err != nil {
					cc.logger.Errorf("error when consuming from Kafka: %s", err)
				}
				if ctx.Err() != nil {
					timer.Stop()
					return
				}
				timer.Reset(topic.Delay)
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}()
}
