### Multiple sets of topics

A consumer can also listen to multiple main topics. This can be done by using an environment variable for each set of
topics and parsing it using this module.

```go
package config

import (
	okconf "github.com/inviqa/kafka-consumer-go/config"
)

const (
	PageTopicsKey    = "price"
	ProductTopicsKey = "product"
)

type Config struct {
	Kafka *okconf.Config
	// other config specific to the service
}

func NewConfig() *Config {
	c := &Config{
		Kafka: okconf.NewConfig(),
		// other config
	}

	c.Kafka.AddTopicsFromStrings(PageTopicsKey, a.KafkaPriceTopics)
	c.Kafka.AddTopicsFromStrings(ProductTopicsKey, a.KafkaProductTopics)

	return c
}
```

The consumer can look up the topic key for the message topic when consuming it in order to decide how to consume it:

```go
package kafka

// ...

func (c *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			switch c.cfg.FindTopicKey(msg.Topic) {
				case config.ProductTopicsKey:
					c.consumeProductUpdate(msg)
				case config.PriceTopicsKey:
					c.consumePriceUpdate(msg)
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
```