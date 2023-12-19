# Using multiple main topics

A consumer can also consume from multiple main topics. This can be done by passing multiple main topics as the source topics in the config builder.

For example, the following config:

```go
consumerCfg, err := config.NewBuilder().
		SetKafkaHost([]string{"broker1"}).
		SetKafkaGroup("algolia").
		SetSourceTopics([]string{"product", "price"}).
		SetRetryIntervals([]int{120}).
		Config()
```

would generate the following topic chains:

1. `price` -> `retry1.algolia.price` (delay of 120 secs) -> `deadLetter.algolia.price`
1. `product` -> `retry1.algolia.product` (delay of 120 secs) -> `deadLetter.algolia.product`

>_NOTE: If DB retries are enabled then retries will be persisted and processed from the database instead of Kafka retry topics, but the same flow applies._

## Multiple topic handlers

If you are consuming messages from multiple main topics, then you will need to provide a topic handler for each of them.

This would look something like:

```go
package main

import (
	okc "github.com/revdaalex/kafka-consumer-go"
	okconf "github.com/revdaalex/kafka-consumer-go/config"
	log "github.com/sirupsen/logrus"
)

func main() {
	// ...

	// set up your handlers...
	ph := kafka.ProductHandler{}
	prh := kafka.PriceHandler{}

	handlerMap := okc.HandlerMap{
		"product": ph.Handle,
		"price":   prh.Handle,
	}

	cfg, err := okconf.NewBuilder().Config()
	if err != nil {
		panic(err)
    }
	okc.Start(cfg, ctx, handlerMap, log.New())
}
```
