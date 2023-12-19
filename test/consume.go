package test

import (
	"context"
	"time"

	consumer "github.com/revdaalex/kafka-consumer-go"
	"github.com/revdaalex/kafka-consumer-go/config"
	"github.com/revdaalex/kafka-consumer-go/log"
)

func ConsumeFromKafkaUntil(cfg *config.Config, hm consumer.HandlerMap, timeout time.Duration, done func(chan<- bool)) error {
	doneCh := make(chan bool)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

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
	if err := consumer.Start(cfg, ctx, hm, log.StdOutLogger{}); err != nil {
		return err
	}
	return nil
}
