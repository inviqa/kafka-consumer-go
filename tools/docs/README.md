# Documentation

## How it works

This module takes care of the consuming of messages from a Kafka cluster, and delegates the processing of those messages to a topic handler function that you provide in your implementation.

If your topic handler function returns an error, e.g. if an upstream service is down and you cannot process the message because of it, then this module will automatically forward on the failed message to a retry topic to be picked up later.

In its simplest configuration, there is a single main topic where messages are consumed from, and a chain of retry topics that messages move through if they error during processing, i.e. the topic handler function returns an error value.

An example topic chain would be something like

`event.product` -> `retry1.groupName.product` -> `deadLetter.groupName.product`

The topic chain is determined by your [configuration].

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

[configuration]: /tools/docs/configuration.md
[implementing a handler]: /tools/docs/implementing-a-handler.md