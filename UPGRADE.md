# Upgrades

This document highlights breaking changes in releases that will require some migration effort in your project. As we move towards a `1.0.0` release these will be restricted to major upgrades only, but currently, whilst the API is still being fleshed out in the `0.x` releases, they may be more frequent. 

## `0.5.x` -> `0.6.0`

* The `test.NewConfig()` helper function has been removed. Instead, just use `config.NewBuilder()` to build your config in your test code.

## `0.4.x` -> `0.5.0`

* The `DB` field in `config.Config` has now been renamed to `db` and is therefore unavailable outside of the `config` package 
* The `data.MigrateDatabase()` function signature has changed: it now no longer needs a `*config.Config` value as its second argument, instead it takes the schema name as a string instead (this should not affect user-land code).
* The `data.NewDB()` function signature has changed: it now no longer needs a `log.Logger` value as its second argument.

## `0.3.x` -> `0.4.0`

* The handler signature has now changed. The first argument is now a `context.Context` value to allow handlers to act upon cancellations and timeouts from this module.
* `config.NewConfig()` function has been removed in favour of using the config builder, via `config.NewBuilder()`. See [the docs](/tools/docs/configuration.md#example-of-builder) for more information on how to use this.
* `test.NewConfig()` argument types have changed to align it with the builder that it now uses underneath the hood:
  * The `sourceTopics` argument is now a `[]string` instead of `string`
  * The `retryIntervals` argument is now a `[]int` instead of `string`
* `config.Builder.SetTopicNameDecorator()` has been renamed to `config.Builder.SetTopicNameGenerator()` which matches the concept and underlying field names better

## `0.2.x` -> `0.3.0`

* `test.ConsumeFromKafkaUntil()` now returns an error if the consuming failed, you will need to handle it if you are using this test helper in your integration test suite
* `consumer.Start()`'s parameters have now changed, there is no need to provide a channel for failures as this is now managed internally
* `consumer.Start()` now returns an error if the consuming failed, you will need to handle this error, likely by logging it and exiting
* `consumer.NewCollection()` has been renamed to `consumer.newKafkaConsumerCollection()`, and is now no longer part of the public API of this module
* The `consumer.ConsumerCollection` interface has been renamed to `consumer.collection`, and is now no longer part of the public API of this module
* The `Failure` struct type has been moved to `data/failure/model` package, previously it was in the module root
* `consumer.NewConsumer()` function has been renamed to `consumer.newConsumer()`, and is now no longer part of the public API of this module.

## `0.1.x` -> `0.2.0`

* `config.NewConfig()` now returns an error value, as well as a `*config.Config` value, so you will need to check for an error and handle it accordingly
* You can now use `test.NewConfig()` to create a `config.Config` value for testing. See [testing](/tools/docs/advanced/testing.md) for more information.
* You can now use the `test.ConsumeFromKafkaUntil()` function in your integration tests to make it easier to wait until messages are consumed and conditions have been satisfied. See [testing](/tools/docs/advanced/testing.md) for more information.