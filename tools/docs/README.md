## Usage

It is expected that the topics contain at least the initial topic to consume from, and a dead-letter topic for any failed messages. Additional retry topics can also be configured inbetween the initial and dead-letter topics. Each topic in the chain, aside from the dead-letter topic, has a configurable retry delay.

## Configuration

See [configuration](/tools/docs/configuration.md) for more on how to configure this module.

## Implementing in your service

See [implementing a handler](/tools/docs/implementing-a-handler.md) for information on how to implement a consumer handler with this module.
