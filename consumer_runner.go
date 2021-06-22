package consumer

import (
	"context"
	"log"
	"sync"

	"github.com/inviqa/kafka-consumer-go/config"
)

func Start(kcfg *config.Config, ctx context.Context, fch chan Failure, hs HandlerMap, l Logger) {
	if l == nil {
		l = NullLogger{}
	}

	wg := &sync.WaitGroup{}

	// create a failure producer
	producer, err := NewFailureProducerWithDefaults(kcfg, fch, l)
	if err != nil {
		log.Panic("could not start Kafka failure producer")
	}

	// create a consumer collection
	cons := NewCollection(kcfg, producer, fch, hs, config.NewSaramaConfig(kcfg.TLSEnable, kcfg.TLSSkipVerifyPeer), l)
	err = cons.Start(ctx, wg)
	if err != nil {
		log.Panic("unable to start consumers")
	}
	defer cons.Close()

	l.Info("kafka consumer started")

	// wait for the consumer to terminate
	wg.Wait()
}
