# Implementing your consumer handler

This module gives you the structure to build out your own consumer handler. This document explains the steps to do that.

## Consumer handler

The consumer handler needs to be created in the implementing service importing this module, and it must implement the `sarama.ConsumerGroupHandler` interface. We recommend puttig this in a `kafka` package. For example:

```go
package kafka

import (
	"github.com/Shopify/sarama"
	okc "github.com/inviqa/kafka-consumer-go"
	okconf "github.com/inviqa/kafka-consumer-go/config"
)

type consumerHandler struct {
	failureCh chan<- *okc.Failure
	cfg       *okconf.Config
}

func NewConsumerHandler(fch chan<- *okc.Failure, cfg *okconf.Config) sarama.ConsumerGroupHandler {
	return &consumerHandler{
		failureCh: fch,
		cfg:       cfg,
	}
}

// ConsumeClaim must start a consumerHandler loop of ConsumerGroupClaim's Messages().
// This runs inside a goroutine, therefore we should not use goroutines within this
// function
func (c *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			if err != nil {
				topic, nextErr := c.cfg.NextTopicNameInChain(message.Topic)
				// handle nextErr if needed, where no next topic can be found in the chain
				c.failureCh <- &okc.Failure{
					Reason:        err.Error(),
					Message:       message.Value,
					TopicToSendTo: topic,
				}
			}

			//The message should be marked even if an error occurred so that the consumer can continue to process records from the topic
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}
```

To create your consumer handler:

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	okc "github.com/inviqa/kafka-consumer-go"
	okconf "github.com/inviqa/kafka-consumer-go/config"
	log "github.com/sirupsen/logrus"
)

func main() {
	// ...
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		cancel()
	}()

	// we use github.com/sirupsen/logrus for our logger
	logger := log.New()
	wg := &sync.WaitGroup{}
	fch := make(chan okc.Failure)
	kafkaCfg := okconf.NewConfig()

	// create your handler:
	handler := kafka.NewConsumerHandler(fch, kafkaCfg)

	// create a failure producer, using the same channel we gave to the consumer handler
	producer, err := okc.NewFailureProducerWithDefaults(kafkaCfg, ctx, fch, logger)

	// create a consumer collection from this module
	cons := okc.NewCollection(kafkaCfg, producer, handler, okconf.NewSaramaConfig(false, false), logger)
	err = cons.Start(ctx, wg)
	if err != nil {
		log.Panic("unable to start consumers")
	}
	defer cons.Close()

	log.Info("kafka consumer started")

	// wait for the consumer to terminate
	wg.Wait()
}
```
