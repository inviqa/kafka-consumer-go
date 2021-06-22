package test

import (
	"context"
	"time"

	consumer "github.com/inviqa/kafka-consumer-go"
	"github.com/inviqa/kafka-consumer-go/config"
)

func ConsumeFromKafkaUntil(cfg *config.Config, hm consumer.HandlerMap, timeout time.Duration, done func(chan<- bool)) {
	doneCh := make(chan bool)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fch := make(chan consumer.Failure)

	go func() {
		defer cancel()
		select {
		case <-doneCh:
			return
		case <-ctx.Done():
			return
		}
	}()

	go done(doneCh)
	consumer.Start(cfg, ctx, fch, hm, nil)
}
