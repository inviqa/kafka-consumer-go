# Upgrades

This document highlights breaking changes in releases that will require some migration effort in your project. As we move towards a `1.0.0` release these will be restricted to major upgrades only, but currently, whilst the API is still being fleshed out in the `0.x` releases, they may be more frequent. 

## `0.2.x` -> `0.3.0`

* `test.ConsumeFromKafkaUntil()` now returns an error if the consuming failed, you will need to handle it if you are using this test helper in your integration test suite
* `consumer.Start()`'s parameters have now changed, there is no need to provide a channel for failures as this is now managed internally
* `consumer.Start()` now returns an error if the consuming failed, you will need to handle this error, likely by logging it and exiting
* `consumer.NewCollection()` has been renamed to `consumer.newKafkaConsumerCollection()`, and is now no longer part of the public API of this module

## `0.1.x` -> `0.2.0`

* `config.NewConfig()` now returns an error value, as well as a `*config.Config` value, so you will need to check for an error and handle it accordingly
* You can now use `test.NewConfig()` to create a `config.Config` value for testing. See [testing](/tools/docs/advanced/testing.md) for more information.
* You can now use the `test.ConsumeFromKafkaUntil()` function in your integration tests to make it easier to wait until messages are consumed and conditions have been satisfied. See [testing](/tools/docs/advanced/testing.md) for more information.