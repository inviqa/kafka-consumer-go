### Multiple sets of topics

A consumer can also listen to multiple main topics. This can be done by passing comma separated list of main topics to the `KAFKA_SOURCE_TOPICS` variable.

For example, the following config:
```
KAFKA_SOURCE_TOPICS=price,product
KAFKA_RETRY_INTERVALS=120
```
would generate topics:
```
price
retry1.kafkaGroup.price (delay:120)
deadLetter.kafkaGroup.price

product
retry1.kafkaGroup.product (delay:120)
deadLetter.kafkaGroup.product
```

Then, you can consume it in consumer as follows:

```go
package config

const (
	PriceTopicsKey    = "price"
	ProductTopicsKey = "product"
)
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