# Implementing your consumer handler

The consuming of messages is handled by this module. You need to implement the processing of each message received from a topic by creating a `consumer.HandlerMap` value, where they key of the map is the topic name of the message that is being processed from Kafka, and the values are the handler functions. This document explains how to do that.

## Topic handler

A topic handler function needs to be created in the implementing service importing this module, and it must have the following signature

    func(ctx context.Context, msg *sarama.ConsumerMessage) error

You can implement this as a standadalone function, or as a method on a receiver. It should return an error if there was a problem processing the message, e.g. if your database returned an error, or if an upstream REST API returned an error and you want to retry it later.

>_NOTE: You should only really return an error value if you want to retry the processing later. If you encounter an error that would not be resolved by a retry, e.g. a `400 Bad Request` response from a REST API, then you may not want to retry it later as it would produce the same response. In this case, you would want to log the error and return a `nil` value from your topic handler._

Here is a simple example of a topic handler implemented as a method on a struct type receiver: 

```go
package kafka

import (
	"database/sql"
	
	"github.com/IBM/sarama"
)

type ProductHandler struct {
	db *sql.DB
}

func (ph productHandler) Handle(msg *sarama.ConsumerMessage) error {
	// handle the given product message
	// return error on failure
	// message would be marked as processed by the actual consumer handler in any case
	// in case of error, the consumer handler would forward this message to the next queue in chain (retry/deadletter)
	_, err := ph.db.Exec(/* ... */)
	
	return err
}
```

## Starting the consumer

Now that you have defined at least one topic handler, you can start your consumer in your `main` function. Below is an example:

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	okc "github.com/revdaalex/kafka-consumer-go"
	okconf "github.com/revdaalex/kafka-consumer-go/config"
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
	
	// set up your handlers...
	ph := kafka.ProductHandler{}

	// set up a logger, we use github.com/sirupsen/logrus for ours
	logger := log.New()
	
	// create your handler map, where the keys are the topic names, and the values are the corresponding handlers
	handlerMap := okc.HandlerMap{
		"product": ph.Handle,
	}

	cfg, err := okconf.NewBuilder().Config()
	if err != nil {
		log.WithError(err).Panic("unable to create consumer configuration")
	}
	
	// start the consumer, which is blocking and will wait until context cancellation
	fch := make(chan okc.Failure)
	okc.Start(cfg, ctx, fch, handlerMap, logger)
}
```

>_NOTE: Make sure you have configured the consumer correctly, by following the [configuration] guide._

## Handler map keys

The keys in the `consumer.HandlerMap` type are the names of the topics where the messages are consumed from. To keep things simple, if a message is pulled in for retry it will be delegated to the same handler as the main topic that it was consumed from initially. In the example above, this would be the `ph.Handle` method, as the `"product"` key in the handler map was the original topic handler.

[configuration]: configuration.md
