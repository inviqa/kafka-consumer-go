# Implementing your consumer handler

The actual consumer handler is already implemented in this module. It calls a closure function provided to it for each topic being consumed.
This module allows you to provide that map of kafka topic and its handler as a closure function. This document explains the steps to do that.

## Topic handler

The topic handler closure function needs to be created in the implementing service importing this module, and it must have the signature `func(msg *sarama.ConsumerMessage) error`. We recommend putting this in a `kafka` package. For example:

```go
package kafka

import (
	"github.com/Shopify/sarama"
)

func ProductTopicHandler(msg *sarama.ConsumerMessage) error {
	// handle the given product message
	// return error on failure
	// message would be marked as processed by the actual consumer handler in any case
	// in case of error, the consumer handler would forward this message to the next queue in chain (retry/deadletter)
	return nil
}
```

To start the consumer handler:

```go
package main

import (
	"context"
	"os"
	"os/signal"
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
	fch := make(chan okc.Failure)
	kafkaCfg := okconf.NewConfig()
	handlerMap := okc.HandlerMap{
		"product": kafka.ProductTopicHandler,
    }
    
	okc.Start(kafkaCfg, ctx, fch, handlerMap, logger)
}
```
