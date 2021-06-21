# Using multiple main topics

A consumer can also consume from multiple main topics. This can be done by passing a comma separated list of main topics to the `KAFKA_SOURCE_TOPICS` variable.

For example, the following config:

```
KAFKA_SOURCE_TOPICS=price,product
KAFKA_RETRY_INTERVALS=120
KAFKA_GROUP=algolia
```

would generate the following topic chains:

1. `price` -> `retry1.algolia.price` (delay of 120 secs) -> `deadLetter.algolia.price`
1. `product` -> `retry1.algolia.product` (delay of 120 secs) -> `deadLetter.algolia.product`

## Multiple topic handlers

If you are consuming messages from multiple main topics, then you will need to provide a topic handler for each of them.

This would look something like:

```go
package main

import (
	okc "github.com/inviqa/kafka-consumer-go"
	okconf "github.com/inviqa/kafka-consumer-go/config"
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

	fch := make(chan okc.Failure)
	okc.Start(okconf.NewConfig(), ctx, fch, handlerMap, log.New())
}
```
