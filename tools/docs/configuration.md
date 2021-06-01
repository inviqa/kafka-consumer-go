# Configuration

To use this module you must configure it so that it knows how to connect to the Kafka cluster. You do this by setting environment variables, which are detailed below.

## Kafka connection

* `KAFKA_HOST`: comma-separated host names for brokers in your cluster, e.g. `KAFKA_HOST=kafka1.eu-central-1.amazonaws.com:9094,kafka2.eu-central-1.amazonaws.com:9094`
* `KAFKA_GROUP`: the name of the consumer group for your service

## Kafka topics

You can use the `KAFKA_SOURCE_TOPICS` environment variable in combination with `KAFKA_RETRY_INTERVALS` to control which topics to consume from in your cluster.
This module generates a set of consume topics with retry intervals based on given environment variables.

For example, the following config:

```
KAFKA_SOURCE_TOPICS=mainTopic
KAFKA_RETRY_INTERVALS=120
```

would generate topics:

```
mainTopic
retry1.kafkaGroup.mainTopic (delay:120)
deadLetter.kafkaGroup.mainTopic
```
You can see it has automatically generated the retry and deadLetter topic names along with the retry delay.

Note: You do not need to have any retry topics in the chain, but it is advisable in most circumstances. 
If you don't set the `KAFKA_RETRY_INTERVALS` variable, then it would directly send the failures to the deadLetter topic.

### Flow of event processing:
Sticking the configuration example above, this will tell this module to:

* Consume records from `mainTopic`
* If there are errors during the processing of those records then publish them to the next topic in the chain: `retry1.kafkaGroup.mainTopic`
* Wait for 120 seconds before processing messages from this topic
* If there are any errors processing these messages, then publish them to the last topic in the chain: `deadLetter.kafkaGroup.mainTopic`

> _NOTE: No consumer will be registered for the last topic in the chain, as this indicates that something has gone wrong and more retries are unlikely to solve it, so it needs manual intervention._

### Multiple sets of topics

See [using multiple main topics](/tools/docs/using-multiple-main-topics.md).
