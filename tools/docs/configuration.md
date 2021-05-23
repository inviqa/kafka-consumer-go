# Configuration

To use this module you must configure it so that it knows how to connect to the Kafka cluster. You do this by setting environment variables, which are detailed below.

## Kafka connection

* `KAFKA_HOST`: comma-separated host names for brokers in your cluster, e.g. `KAFKA_HOST=kafka1.eu-central-1.amazonaws.com:9094,kafka2.eu-central-1.amazonaws.com:9094`
* `KAFKA_GROUP`: the name of the consumer group for your service

## Kafka topics

There are two ways to configure kafka topics.

### Using automated retry and deadLetter topic generation:

You cann use the `KAFKA_SOURCE_TOPICS` environment variable in combination with `KAFKA_RETRY_INTERVALS` to control which topics to consume from in your cluster.
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

### Manually providing the topic names and delay:

This is the old way of configuring the kafka topics in this module. 

You can use the `KAFKA_TOPICS` environment variable to control which topics to consume from in your cluster.

If only one set of topics needs to be consumed then the following config can be used:

```
KAFKA_TOPICS=mainTopic:0,retryTopic:120:deadletterTopic
```

### Format of `KAFKA_TOPICS`

The string used for `KAFKA_TOPICS` is a comma separated list starting with the first topic to consume from, followed by retry topics and finally a dead letter topic. You do not need to have any retry topics in the chain, but it is advisable in most circumstances.

Sticking the example above, this will tell this module to:

Where:
* Consume records from `mainTopic`
* If there are errors during the processing of those records then publish them to the next topic in the chain: `retryTopic`
* Wait for 120 seconds before processing messages from this topic
* If there are any errors processing these messages, then publish them to the last topic in the chain: `deadLetterTopic`

> _NOTE: No consumer will be registered forr the last topic in the chain, as this indicates that something has gone wrong and more retries are unlikely to solve it, so it needs manual intervention._

### Multiple sets of topics

See [using multiple main topics](/tools/docs/using-multiple-main-topics.md).
