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
	"github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/log"
	"github.com/inviqa/kafka-consumer-go/test/saramatest"
)

func TestNewCollection(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	cfg := &config.Config{}
	fp := newKafkaFailureProducer(saramatest.NewMockSyncProducer(), make(chan model.Failure, 10), nil)
	fch := make(chan model.Failure)
	scfg := config.NewSaramaConfig(false, false)
	l := log.NullLogger{}
	hm := HandlerMap{}

	exp := &kafkaConsumerCollection{
		cfg:            cfg,
		consumers:      []sarama.ConsumerGroup{},
		producer:       fp,
		handler:        newConsumer(fch, cfg, hm, l),
		saramaCfg:      scfg,
		logger:         l,
		connectToKafka: defaultKafkaConnector,
	}
	got := newKafkaConsumerCollection(cfg, fp, fch, hm, scfg, nil, defaultKafkaConnector)

	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

func TestCollection_Close(t *testing.T) {
	t.Run("it closes consumers", func(t *testing.T) {
		t.Parallel()
		mcg1 := saramatest.NewMockConsumerGroup()
		mcg2 := saramatest.NewMockConsumerGroup()
		col := &kafkaConsumerCollection{consumers: []sarama.ConsumerGroup{mcg1, mcg2}}

		col.Close()

		if !mcg1.WasClosed() || !mcg2.WasClosed() {
			t.Errorf("consumer collection was not closed properly")
		}
	})

	t.Run("it handles errors from closing consumers", func(t *testing.T) {
		t.Parallel()
		mcg1 := saramatest.NewMockConsumerGroup()
		mcg2 := saramatest.NewMockConsumerGroup()
		mcg1.ErrorOnClose()

		col := &kafkaConsumerCollection{consumers: []sarama.ConsumerGroup{mcg1, mcg2}, logger: log.NullLogger{}}
		col.Close()

		if !mcg2.WasClosed() || len(col.consumers) > 0 {
			t.Errorf("consumer collection was not closed properly")
		}
	})
}

func TestCollection_Start(t *testing.T) {
	exampleMsg := &sarama.ConsumerMessage{
		Topic: "product",
		Value: []byte(`{"foo":"bar"}`),
	}

	t.Run("errors when there are no main topics", func(t *testing.T) {
		t.Parallel()
		col := kafkaConsumerCollection{cfg: &config.Config{}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.Start(ctx, &wg); err == nil {
			t.Error("expected an error but got nil")
		}
		wg.Wait()
	})

	t.Run("errors when it cannot connect to kafka", func(t *testing.T) {
		t.Parallel()
		col, _ := testKafkaConsumerCollection(saramatest.NewMockConsumerGroup(), nil, true)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.Start(ctx, &wg); err == nil {
			t.Error("expected an error but got nil")
		}
		wg.Wait()
	})

	t.Run("handles errors on kafka consume", func(t *testing.T) {
		t.Parallel()
		mcg := saramatest.NewMockConsumerGroup()
		mcg.ErrorOnConsume()
		col, _ := testKafkaConsumerCollection(mcg, nil, false)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		var wg sync.WaitGroup
		// errors from consume should be logged, but not returned
		if err := col.Start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if !mcg.Consumed() {
			t.Error("expected something to have been consumed, but was not")
		}
	})

	t.Run("successful messages are not retried", func(t *testing.T) {
		t.Parallel()
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		col, failureProd := testKafkaConsumerCollection(mcg, func(msg *sarama.ConsumerMessage) error {
			return nil
		}, false)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()

		var wg sync.WaitGroup
		if err := col.Start(ctx, &wg); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		wg.Wait()

		if got := failureProd.lastReceivedFailure(); got != nil {
			t.Errorf("did not expect a failure, but got %#v", got)
		}

		if c := mcg.GetTopicConsumeCount("product"); c == 0 {
			t.Error("expected topic 'product' to be consumed, but was not")
		}
	})

	t.Run("failing messages are sent for retry", func(t *testing.T) {
		t.Parallel()
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		var called bool
		col, failureProd := testKafkaConsumerCollection(mcg, func(msg *sarama.ConsumerMessage) error {
			if !called {
				called = true
				return errors.New("something bad happened")
			}
			return nil
		}, false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := col.Start(ctx, &sync.WaitGroup{}); err != nil {
			t.Errorf("unexpected error: %s", err)
		}

	L:
		for {
			select {
			case <-time.After(time.Second * 1):
				t.Error("did not receive any failures in time")
			default:
				if failureProd.numberOfReceivedFailures() == 1 {
					cancel()
					break L
				}
			}
		}

		if got := failureProd.numberOfReceivedFailures(); got != 1 {
			t.Errorf("expected only 1 failure to be produced, but got %d", got)
		}
	})
}

func testKafkaConsumerCollection(mcg *saramatest.MockConsumerGroup, msgHandler Handler, errorOnConnect bool) (*kafkaConsumerCollection, *mockFailureProducer) {
	fch := make(chan model.Failure, 10)

	hm := HandlerMap{"product": msgHandler}
	connector := testKafkaConnector{consumerGroup: mcg, willError: errorOnConnect}

	mockFp := newMockFailureProducer(fch)

	return newKafkaConsumerCollection(newTestConfig(), mockFp, fch, hm, sarama.NewConfig(), log.NullLogger{}, connector.connectToKafka), mockFp
}
