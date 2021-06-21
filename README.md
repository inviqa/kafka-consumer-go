# Go Kafka Consumer

This is a module that provides Kafka consumer and retry logic for services. It registers a new consumer on a configurable list of topics and handles the retrying of messsages using a topic chain.

## Installation

It can be installed into a concrete consumer service using:

```
go get github.com/inviqa/kafka-consumer-go
```

## Documentation

See the [docs](/tools/docs) on how to use this Go module.

## Changelog

This project maintains a changelog, which you can find [here](/CHANGELOG.md).

## Running tests

This module ships with unit tests. You can run these with `go test ./...`.

# Performing a release

See the [releasing docs](/tools/docs/releasing.md) for info.

# License

Copyright 2021, Inviqa

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
