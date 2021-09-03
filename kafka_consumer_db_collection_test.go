package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/log"
	"github.com/inviqa/kafka-consumer-go/test/saramatest"
)

func TestNewKafkaConsumerDbCollection(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	cfg := &config.Config{}
	repo := newMockRetriesRepository(false)
	fch := make(chan data.Failure)
	dp := newDatabaseProducer(repo, fch, nil)
	hm := HandlerMap{}
	scfg := config.NewSaramaConfig(false, false)
	logger := log.NullLogger{}

	exp := &kafkaConsumerDbCollection{
		kafkaConsumers: []sarama.ConsumerGroup{},
		cfg:            cfg,
		producer:       dp,
		repo:           repo,
		handler:        NewConsumer(fch, cfg, hm, logger),
		handlerMap:     hm,
		saramaCfg:      scfg,
		logger:         logger,
		connectToKafka: defaultKafkaConnector,
	}

	got := newKafkaConsumerDbCollection(cfg, dp, repo, fch, hm, scfg, logger, defaultKafkaConnector)

	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

func TestKafkaConsumerDbCollection_Start(t *testing.T) {
	defaultDbRetryPollInterval := dbRetryPollInterval
	dbRetryPollInterval = time.Millisecond * 25
	defer func() {
		dbRetryPollInterval = defaultDbRetryPollInterval
	}()

	exampleMsg := &sarama.ConsumerMessage{
		Topic: "product",
		Value: []byte(`{"foo":"bar"}`),
	}

	t.Run("errors when there are no main topics", func(t *testing.T) {
		col := kafkaConsumerDbCollection{cfg: &config.Config{}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1)
		defer cancel()
		if err := col.Start(ctx, &sync.WaitGroup{}); err == nil {
			t.Error("expected an error but got nil")
		}
	})

	t.Run("successful messages are not retried", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		col, repo := kafkaConsumerDbCollectionForTests(mcg, func(msg *sarama.ConsumerMessage) error {
			return nil
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.Start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if got := repo.getPublishedFailureCountByTopic("product"); got != 0 {
			t.Errorf("expected 0 failures to be produced in database, but got %d", got)
		}
	})

	t.Run("retries are marked successful when they eventually succeed", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		var called bool
		col, repo := kafkaConsumerDbCollectionForTests(mcg, func(msg *sarama.ConsumerMessage) error {
			if !called {
				called = true
				return errors.New("something bad happened")
			}
			return nil
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.Start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if got := repo.getPublishedFailureCountByTopic("product"); got != 1 {
			t.Errorf("expected 1 failure to be produced in database, but got %d", got)
		}

		if !repo.retrySuccessful {
			t.Error("expected the DB retry to have been marked as successful, but it wasn't")
		}
	})

	t.Run("retries are marked as errored when they continue to fail", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		col, repo := kafkaConsumerDbCollectionForTests(mcg, func(msg *sarama.ConsumerMessage) error {
			return errors.New("something bad happened")
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.Start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if got := repo.getPublishedFailureCountByTopic("product"); got != 1 {
			t.Errorf("expected 1 failure to be produced in database, but got %d", got)
		}

		if !repo.retryErrored {
			t.Error("expected the DB retry to have been marked as successful, but it wasn't")
		}
	})
}

func kafkaConsumerDbCollectionForTests(mcg *saramatest.MockConsumerGroup, msgHandler Handler) (*kafkaConsumerDbCollection, *mockRetriesRepository) {
	fch := make(chan data.Failure, 10)
	repo := newMockRetriesRepository(false)
	dp := newDatabaseProducer(repo, fch, log.NullLogger{})

	hm := HandlerMap{"product": msgHandler}
	connector := testKafkaConnector{consumerGroup: mcg}

	return newKafkaConsumerDbCollection(newTestConfig(), dp, repo, fch, hm, sarama.NewConfig(), log.NullLogger{}, connector.connectToKafka), repo
}