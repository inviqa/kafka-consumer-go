package consumer

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data/failure"
	"github.com/inviqa/kafka-consumer-go/log"
	"github.com/inviqa/kafka-consumer-go/test/saramatest"
)

func TestNewCollection(t *testing.T) {
	cfg := &config.Config{}
	fp := newKafkaFailureProducer(saramatest.NewMockSyncProducer(), make(chan failure.Failure, 10), nil)
	fch := make(chan failure.Failure)
	scfg := config.NewSaramaConfig(false, false)
	l := log.NullLogger{}
	hm := HandlerMap{}

	exp := &kafkaConsumerCollection{
		cfg:       cfg,
		consumers: []sarama.ConsumerGroup{},
		producer:  fp,
		handler:   NewConsumer(fch, cfg, hm, l),
		saramaCfg: scfg,
		logger:    l,
	}
	col := newKafkaConsumerCollection(cfg, fp, fch, hm, scfg, nil)
	if !reflect.DeepEqual(exp, col) {
		t.Errorf("expected %v, but got %v", exp, col)
	}
}

func TestCollection_Close(t *testing.T) {
	mcg1 := saramatest.NewMockConsumerGroup()
	mcg2 := saramatest.NewMockConsumerGroup()
	col := &kafkaConsumerCollection{consumers: []sarama.ConsumerGroup{mcg1, mcg2}}

	col.Close()

	if !mcg1.WasClosed() || !mcg2.WasClosed() {
		t.Errorf("consumer collection was not closed properly")
	}
}

func TestCollection_CloseWithError(t *testing.T) {
	mcg1 := saramatest.NewMockConsumerGroup()
	mcg2 := saramatest.NewMockConsumerGroup()
	mcg1.ErrorOnClose()

	col := &kafkaConsumerCollection{consumers: []sarama.ConsumerGroup{mcg1, mcg2}, logger: log.NullLogger{}}
	col.Close()

	if !mcg2.WasClosed() || len(col.consumers) > 0 {
		t.Errorf("consumer collection was not closed properly")
	}
}

func TestCollection_startConsumer(t *testing.T) {
	mcg := saramatest.NewMockConsumerGroup()
	ctx := context.Background()
	cfg := &config.Config{}

	col := &kafkaConsumerCollection{consumers: []sarama.ConsumerGroup{mcg}, cfg: cfg}

	col.startConsumer(mcg, ctx, &sync.WaitGroup{}, &config.KafkaTopic{
		Name:  "retry",
		Delay: time.Millisecond * 1,
	})

	<-time.After(time.Millisecond * 5)

	if c := mcg.GetTopicConsumeCount("retry"); c == 0 {
		t.Error("expected topic 'retry' to be consumed, but was not")
	}
}

func TestCollection_startConsumerWithError(t *testing.T) {
	mcg := saramatest.NewMockConsumerGroup()
	mcg.ErrorOnConsume()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := &config.Config{}

	col := &kafkaConsumerCollection{consumers: []sarama.ConsumerGroup{mcg}, cfg: cfg}

	col.startConsumer(mcg, ctx, &sync.WaitGroup{}, &config.KafkaTopic{
		Name:  "retry",
		Delay: time.Millisecond * 1,
	})

	cancel()
}
