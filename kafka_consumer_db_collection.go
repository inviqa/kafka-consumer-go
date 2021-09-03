package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries"
	"github.com/inviqa/kafka-consumer-go/log"
)

// kafkaConsumerDbCollection is a collection of consumers that initially consume messages from Kafka
// but then process retries from a database table instead. The failureProducer used by this collection
// should be a databaseProducer.
type kafkaConsumerDbCollection struct {
	cfg            *config.Config
	kafkaConsumers []sarama.ConsumerGroup
	producer       failureProducer
	repo           retryRepository
	handler        sarama.ConsumerGroupHandler
	handlerMap     HandlerMap
	saramaCfg      *sarama.Config
	logger         log.Logger
	connectToKafka kafkaConnector
}

type kafkaConnector func(cfg *config.Config, saramaCfg *sarama.Config, logger log.Logger) (sarama.ConsumerGroup, error)

type retryRepository interface {
	GetMessagesForRetry(topic string, sequence uint8, interval time.Duration) ([]retries.Retry, error)
	MarkRetrySuccessful(id int64) error
	MarkRetryErrored(retry retries.Retry) error
}

func newKafkaConsumerDbCollection(
	cfg *config.Config,
	p failureProducer,
	repo retryRepository,
	fch chan data.Failure,
	hm HandlerMap,
	scfg *sarama.Config,
	logger log.Logger,
	connector kafkaConnector,
) *kafkaConsumerDbCollection {
	if logger == nil {
		logger = log.NullLogger{}
	}

	return &kafkaConsumerDbCollection{
		cfg:            cfg,
		kafkaConsumers: []sarama.ConsumerGroup{},
		producer:       p,
		repo:           repo,
		handler:        NewConsumer(fch, cfg, hm, logger),
		handlerMap:     hm,
		saramaCfg:      scfg,
		logger:         logger,
		connectToKafka: connector,
	}
}

func (cc *kafkaConsumerDbCollection) Start(ctx context.Context, wg *sync.WaitGroup) error {
	topics := cc.cfg.MainTopics()
	if topics == nil || len(topics) == 0 {
		return errors.New("no Kafka topics are configured, therefore cannot start consumers")
	}

	for _, t := range topics {
		group, err := cc.startMainTopicConsumer(ctx, wg, t)
		if err != nil {
			return err
		}
		cc.kafkaConsumers = append(cc.kafkaConsumers, group)
		cc.startDbRetryProcessorsForTopic(ctx, t, cc.cfg.DBRetries[t], wg)
	}
	cc.producer.listenForFailures(ctx, wg)

	return nil
}

// startMainTopicConsumer starts a sarama.ConsumerGroup to consume messages from Kafka for the given main topic name
func (cc *kafkaConsumerDbCollection) startMainTopicConsumer(ctx context.Context, wg *sync.WaitGroup, topic string) (sarama.ConsumerGroup, error) {
	cc.logger.Infof("starting Kafka consumer group for '%s'", topic)

	cl, err := cc.connectToKafka(cc.cfg, cc.saramaCfg, cc.logger)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range cl.Errors() {
			cc.logger.Errorf("error occurred in consumer group Handler: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := cl.Consume(ctx, []string{topic}, cc.handler); err != nil {
					cc.logger.Errorf("error when consuming from Kafka: %s", err)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
	}()

	return cl, nil
}

func (cc *kafkaConsumerDbCollection) startDbRetryProcessorsForTopic(ctx context.Context, topic string, retryConfig []*config.DBTopicRetry, wg *sync.WaitGroup) {
	for _, rc := range retryConfig {
		wg.Add(1)
		go func(retryConfig *config.DBTopicRetry) {
			defer wg.Done()
			timer := time.NewTimer(dbRetryPollInterval)
			for {
				select {
				case <-timer.C:
					cc.processMessagesForRetry(topic, retryConfig)
					timer.Reset(dbRetryPollInterval)
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				}
				time.Sleep(dbRetryPollInterval)
			}
		}(rc)
	}
}

func (cc *kafkaConsumerDbCollection) processMessagesForRetry(topic string, rc *config.DBTopicRetry) {
	msgsForRetry, err := cc.repo.GetMessagesForRetry(topic, rc.Sequence, rc.Interval)
	if err != nil {
		cc.logger.Errorf("error when fetching messages from the DB for retry: %s", err)
		return
	}

	h, ok := cc.handlerMap[rc.Key]
	if !ok {
		cc.logger.Errorf("no handler found for topic key '%s'", rc.Key)
	}

	for _, msg := range msgsForRetry {
		saramaMsg := msg.ToSaramaConsumerMessage()
		if err = h(saramaMsg); err != nil {
			cc.logger.Errorf("error processing retried message from DB: %s", err)
			if err = cc.repo.MarkRetryErrored(msg); err != nil {
				cc.logger.Errorf("error marking retried message as errored in the DB: %s", err)
			}
		} else {
			cc.logger.Infof("successfully processed retried message from topic '%s' with original partition %d and offset %d", topic, msg.KafkaPartition, msg.KafkaOffset)
			if err = cc.repo.MarkRetrySuccessful(msg.ID); err != nil {
				cc.logger.Errorf("error marking retried message as successful in the DB: %s", err)
			}
		}
	}
}

func (cc *kafkaConsumerDbCollection) Close() {
	// TODO: extract to remove duplication??
	for _, c := range cc.kafkaConsumers {
		if err := c.Close(); err != nil {
			cc.logger.Errorf("error occurred closing a Kafka consumer: %w", err)
		}
	}
	cc.kafkaConsumers = []sarama.ConsumerGroup{}
}
