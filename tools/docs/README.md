# Documentation

## How it works

This module takes care of the consuming of messages from a Kafka cluster, and delegates the processing of those messages to a topic handler function that you provide in your implementation.

If your topic handler function returns an error, e.g. if an upstream service is down and you cannot process the message because of it, then this module will automatically forward on the failed message to a retry topic to be picked up later.

In its simplest configuration, there is a single main topic where messages are consumed from, and a chain of retry intervals that messages move through if they error during processing, i.e. the topic handler function returns an error value.

## Retries

Retries are used to attempt 1 or more additional attempts to process a message, and reduce the need for manual intervention for things like temporary network errors or transient failures upstream.

They can be configured as Kafka topics, or as a database table. See [configuration] on how to do this. When configured as Kafka retries each message that errors will be sent back to Kafka to a special topic where messages get consumed from and retried after a short-interval. If retries continue to fail, then the message will be placed in a dead-letter topic for manual inspection.

If you configure your consumer to use the database for retries, then you will need to provide some additional [configuration] to tell it how to connect to your database. Now, any messages that fail processing will be stored in a database table for retry later. The same rules apply in that if your message continues to fail and runs out of retry attempts, it will be marked as dead-lettered for manual inspection.

### Example

An example retry chain using Kafka as the retry storage would be something like:

`event.product` -> `retry1.groupName.product` -> `deadLetter.groupName.product`

>_NOTE: The topic chain is determined by your [configuration]._

When configured to use a database for retries, that topic chain will be managed purely by tracking the number of attempts to process the message against the correlating database record for it.

## Should I use the database or Kafka for retries?

When this module was originally created, the only option for processing retries was to use additional Kafka topics. However, you may face session timeout issues with Kafka depending on your retry topic configuration, so sometimes it is better to use the database for storing your retries instead as this does not suffer from that problem. 

Currently, **our recommendation is that you continue to use Kafka for retries** until database retries have been tested more thoroughly on real-world projects.

## Getting started

This module should be imported into your Go service using:

    go get github.com/inviqa/kafka-consumer-go

You will now need to configure the consumer, see [configuration] for more on this.

The next step will be to start to write your topic handlers. See [implementing a handler] for information on how to do this.

## Configuration

See [configuration].

## Advanced topics

* [Using multiple main topics](advanced/using-multiple-main-topics.md)
* [Customising the topic naming](advanced/custom-topic-naming.md)
* [Testing](advanced/testing.md)
* [Prometheus](advanced/prometheus.md)

[configuration]: /tools/docs/configuration.md
[implementing a handler]: /tools/docs/implementing-a-handler.md