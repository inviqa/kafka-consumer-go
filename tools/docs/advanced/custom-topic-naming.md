# Custom topic naming

This module provides a default naming strategy for automatically generated retry and dead-letter topic names, but sometimes you may want to customise these. This document explains how.

## Default strategy

### Retry topics

These are made up of the Kafka consumer group name, the retry topic number in the chain, and the original main topic that the message was consumed from. For example, with the following config:

    KAFKA_SOURCE_TOPICS=product
    KAFKA_RETRY_INTERVALS=60,120
    KAFKA_GROUP=algolia

We would get 2 retry topics: `retry1.algolia.product` and `retry2.algolia.product`.

### Dead-letter topics

For these topics, it is a bit simpler. There is only one dead-letter topic per main topic and consumer group pair. For example, with the following config:

    KAFKA_SOURCE_TOPICS=price
    KAFKA_RETRY_INTERVALS=60,120
    KAFKA_GROUP=algolia

We would get a single dead-letter topic: `deadLetter.algolia.price`.

## Customising the topic naming

It is possible to provide a custom function for generating the topic names, during the config creation. For example

```go
package main

import (
	"fmt"

	okc "github.com/inviqa/kafka-consumer-go"
	okconf "github.com/inviqa/kafka-consumer-go/config"
	log "github.com/sirupsen/logrus"
)

func main() {
	// ...

	builder := okconf.NewBuilder().SetTopicNameDecorator(func(group, mainTopic, prefix string) string {
		// produce something like "algolia.retry1.product" instead
		return fmt.Sprintf("%s.%s.%s", group, prefix, mainTopic)
	})

	cfg, err := builder.Config()
	if err != nil {
		log.WithError(err).Panic("unable to create consumer configuration")
	}

	// ...
}
```
