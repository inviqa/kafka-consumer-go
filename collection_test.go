package consumer

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/test"
)

func TestNewCollection(t *testing.T) {
	cfg := &config.Config{}
	fp := NewFailureProducer(context.Background(), test.NewMockSyncProducer(), make(chan Failure, 10), nil)
	fch := make(chan Failure)
	scfg := config.NewSaramaConfig(false, false)
	l := nullLogger{}
	hm := HandlerMap{}

	exp := &Collection{
		cfg:       cfg,
		consumers: []sarama.ConsumerGroup{},
		producer:  fp,
		handler:   NewConsumer(fch, cfg, hm, l),
		saramaCfg: scfg,
		logger:    l,
	}
	col := NewCollection(cfg, fp, fch, hm, scfg, nil)
	if !reflect.DeepEqual(exp, col) {
		t.Errorf("expected %v, but got %v", exp, col)
	}
}

func TestCollection_Close(t *testing.T) {
	mcg1 := test.NewMockConsumerGroup()
	mcg2 := test.NewMockConsumerGroup()
	col := &Collection{consumers: []sarama.ConsumerGroup{mcg1, mcg2}}

	col.Close()

	if !mcg1.WasClosed() || !mcg2.WasClosed() {
		t.Errorf("consumer collection was not closed properly")
	}
}

func TestCollection_CloseWithError(t *testing.T) {
	mcg1 := test.NewMockConsumerGroup()
	mcg2 := test.NewMockConsumerGroup()
	mcg1.ErrorOnClose()

	col := &Collection{consumers: []sarama.ConsumerGroup{mcg1, mcg2}, logger: nullLogger{}}
	col.Close()

	if !mcg2.WasClosed() || len(col.consumers) > 0 {
		t.Errorf("consumer collection was not closed properly")
	}
}

func TestCollection_StartConsumer(t *testing.T) {
	mcg := test.NewMockConsumerGroup()
	ctx := context.Background()
	cfg := &config.Config{}

	col := &Collection{consumers: []sarama.ConsumerGroup{mcg}, cfg: cfg}

	col.startConsumer(mcg, ctx, &sync.WaitGroup{}, &config.KafkaTopic{
		Name:  "retry",
		Delay: time.Millisecond * 1,
	})

	<-time.After(time.Millisecond * 5)

	if c := mcg.GetTopicConsumeCount("retry"); c == 0 {
		t.Error("expected topic 'retry' to be consumed, but was not")
	}
}

func TestCollection_StartConsumerWithError(t *testing.T) {
	mcg := test.NewMockConsumerGroup()
	mcg.ErrorOnConsume()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := &config.Config{}

	col := &Collection{consumers: []sarama.ConsumerGroup{mcg}, cfg: cfg}

	col.startConsumer(mcg, ctx, &sync.WaitGroup{}, &config.KafkaTopic{
		Name:  "retry",
		Delay: time.Millisecond * 1,
	})

	cancel()
}
