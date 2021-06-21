# Configuration

To use this module you must configure it correctly. You do this by setting environment variables, which are detailed below.

Configuration is initialised by calling `config.NewConfig()` in this module, which automatically parses environment variables and returns a `config.Config` value.

## Kafka connection

* `KAFKA_HOST`: comma-separated host names for brokers in your cluster, e.g. `KAFKA_HOST=kafka1.eu-central-1.amazonaws.com:9094,kafka2.eu-central-1.amazonaws.com:9094`
* `KAFKA_GROUP`: the name of the consumer group for your service

## Kafka topics

You can use the `KAFKA_SOURCE_TOPICS` environment variable in combination with `KAFKA_RETRY_INTERVALS` to control which topics to consume from in your cluster.
This module generates a chain of topics with retry intervals based on given environment variables.

For example, the following config:

```
KAFKA_SOURCE_TOPICS=product
KAFKA_RETRY_INTERVALS=120
KAFKA_GROUP=algolia
```

would generate a topic chain of

`product` -> `retry1.algolia.product` (delay of 120 secs) -> `deadLetter.algolia.product`

You can see it has automatically generated the retry and deadLetter topic names along with the retry delay.

>_NOTE: You do not need to have any retry topics in the chain, but it is advisable in most circumstances. 
If you don't set the `KAFKA_RETRY_INTERVALS` variable, then it would directly send the failures to the deadLetter topic._

### Flow of event processing:

Sticking the configuration example above, this will tell this module to:

* Consume records from `product`
* If there are errors during the processing of those records then publish them to the next topic in the chain: `retry1.algolia.product`
* Wait for 120 seconds before processing messages from this topic
* If there are any errors processing these messages, then publish them to the last topic in the chain: `deadLetter.algolia.product`

> _NOTE: No consumer will be registered for the last topic in the chain, as this indicates that something has gone wrong and more retries are unlikely to solve it, so it needs manual intervention._

### Multiple sets of topics

See [using multiple main topics](advanced/using-multiple-main-topics.md).
