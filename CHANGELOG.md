# Changelog

## [v0.6.0](https://github.com/inviqa/kafka-consumer-go/tree/v0.6.0) (2023-02-07)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.5.0...v0.6.0)

**Merged pull requests:**

- handling more than 1 retries [\#49](https://github.com/inviqa/kafka-consumer-go/pull/49) ([bertalan-kis](https://github.com/bertalan-kis))
- Update samara ErrOutOfBrokers check to cope with wrapping [\#48](https://github.com/inviqa/kafka-consumer-go/pull/48) ([andytson-inviqa](https://github.com/andytson-inviqa))
- Support specifying the driver [\#42](https://github.com/inviqa/kafka-consumer-go/pull/42) ([dantleech](https://github.com/dantleech))
- Remove test.NewConfig\(\) helper [\#41](https://github.com/inviqa/kafka-consumer-go/pull/41) ([jameshalsall](https://github.com/jameshalsall))
- Increase max connection attempts to 20 [\#40](https://github.com/inviqa/kafka-consumer-go/pull/40) ([jameshalsall](https://github.com/jameshalsall))

## [v0.5.0](https://github.com/inviqa/kafka-consumer-go/tree/v0.5.0) (2022-03-08)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.4.0...v0.5.0)

**Implemented enhancements:**

- Add dead-lettered prometheus gauge [\#38](https://github.com/inviqa/kafka-consumer-go/pull/38) ([jameshalsall](https://github.com/jameshalsall))
- Memoize the DB connection pool as part of the config [\#37](https://github.com/inviqa/kafka-consumer-go/pull/37) ([jameshalsall](https://github.com/jameshalsall))

## [v0.4.0](https://github.com/inviqa/kafka-consumer-go/tree/v0.4.0) (2022-03-02)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.3.5...v0.4.0)

**Implemented enhancements:**

- Remove env vars used for configuration and inject as part of NewConfig\(\) [\#33](https://github.com/inviqa/kafka-consumer-go/issues/33)
- Use builder exclusively for configuration creation [\#35](https://github.com/inviqa/kafka-consumer-go/pull/35) ([jameshalsall](https://github.com/jameshalsall))
- Pass session context to topic handlers [\#34](https://github.com/inviqa/kafka-consumer-go/pull/34) ([jameshalsall](https://github.com/jameshalsall))
- Encapsulate logic for determining handler for topic key [\#32](https://github.com/inviqa/kafka-consumer-go/pull/32) ([jameshalsall](https://github.com/jameshalsall))

## [v0.3.5](https://github.com/inviqa/kafka-consumer-go/tree/v0.3.5) (2022-02-25)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.3.4...v0.3.5)

**Fixed bugs:**

- Use a unique migrations table name [\#30](https://github.com/inviqa/kafka-consumer-go/pull/30) ([jameshalsall](https://github.com/jameshalsall))

## [v0.3.4](https://github.com/inviqa/kafka-consumer-go/tree/v0.3.4) (2022-02-23)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.3.3...v0.3.4)

**Fixed bugs:**

- Change last\_error to text type to allow full error message [\#29](https://github.com/inviqa/kafka-consumer-go/pull/29) ([jameshalsall](https://github.com/jameshalsall))

## [v0.3.3](https://github.com/inviqa/kafka-consumer-go/tree/v0.3.3) (2022-02-21)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.3.2...v0.3.3)

**Fixed bugs:**

- Properly encode user/pass in DSN [\#28](https://github.com/inviqa/kafka-consumer-go/pull/28) ([jameshalsall](https://github.com/jameshalsall))

## [v0.3.2](https://github.com/inviqa/kafka-consumer-go/tree/v0.3.2) (2022-02-21)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.3.1...v0.3.2)

**Fixed bugs:**

- Log the error from DB ping check properly [\#27](https://github.com/inviqa/kafka-consumer-go/pull/27) ([jameshalsall](https://github.com/jameshalsall))

## [v0.3.1](https://github.com/inviqa/kafka-consumer-go/tree/v0.3.1) (2022-02-18)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.3.0...v0.3.1)

**Implemented enhancements:**

- Add CodeQL to github actions flow [\#21](https://github.com/inviqa/kafka-consumer-go/pull/21) ([jameshalsall](https://github.com/jameshalsall))

**Fixed bugs:**

- Return error when Kafka cluster never becomes available in time [\#26](https://github.com/inviqa/kafka-consumer-go/pull/26) ([jameshalsall](https://github.com/jameshalsall))
- Ensure database is migrated when Start\(\) is called [\#23](https://github.com/inviqa/kafka-consumer-go/pull/23) ([jameshalsall](https://github.com/jameshalsall))

**Security fixes:**

- Upgrade two modules with security vulnerabilities [\#22](https://github.com/inviqa/kafka-consumer-go/pull/22) ([jameshalsall](https://github.com/jameshalsall))

**Merged pull requests:**

- Wait longer between connection attempts to Kafka [\#25](https://github.com/inviqa/kafka-consumer-go/pull/25) ([jameshalsall](https://github.com/jameshalsall))

## [v0.3.0](https://github.com/inviqa/kafka-consumer-go/tree/v0.3.0) (2021-11-08)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.2.0...v0.3.0)

**Implemented enhancements:**

- Provide ability to use a DB storage mechanism for retry and deadletter messages [\#4](https://github.com/inviqa/kafka-consumer-go/issues/4)

**Merged pull requests:**

- Reduce number of goroutines and consumers for DB kafka collection [\#20](https://github.com/inviqa/kafka-consumer-go/pull/20) ([jameshalsall](https://github.com/jameshalsall))
- Add test case for fail/succeed DB retry scenario [\#19](https://github.com/inviqa/kafka-consumer-go/pull/19) ([jameshalsall](https://github.com/jameshalsall))
- Add maintenance job to the DB consumer collection [\#18](https://github.com/inviqa/kafka-consumer-go/pull/18) ([jameshalsall](https://github.com/jameshalsall))
- Upgrade module dependencies [\#17](https://github.com/inviqa/kafka-consumer-go/pull/17) ([jameshalsall](https://github.com/jameshalsall))
- Add support for DB retry queues as alternative to Kafka topics [\#16](https://github.com/inviqa/kafka-consumer-go/pull/16) ([jameshalsall](https://github.com/jameshalsall))
- Switch changelog generator docker image [\#15](https://github.com/inviqa/kafka-consumer-go/pull/15) ([kierenevans](https://github.com/kierenevans))

## [v0.2.0](https://github.com/inviqa/kafka-consumer-go/tree/v0.2.0) (2021-08-06)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/v0.1.0...v0.2.0)

**Implemented enhancements:**

- Add full integration test suite [\#5](https://github.com/inviqa/kafka-consumer-go/issues/5)
- Provide ability to set context on mock sarama session [\#13](https://github.com/inviqa/kafka-consumer-go/pull/13) ([jameshalsall](https://github.com/jameshalsall))
- Improve testing DX [\#12](https://github.com/inviqa/kafka-consumer-go/pull/12) ([jameshalsall](https://github.com/jameshalsall))
- Update config API, provide ability for custom topic naming [\#10](https://github.com/inviqa/kafka-consumer-go/pull/10) ([jameshalsall](https://github.com/jameshalsall))

**Fixed bugs:**

- Reuse timer and stop correctly to prevent memory leak [\#14](https://github.com/inviqa/kafka-consumer-go/pull/14) ([jameshalsall](https://github.com/jameshalsall))

**Merged pull requests:**

- Remove github.com/alexflint/go-arg [\#11](https://github.com/inviqa/kafka-consumer-go/pull/11) ([jameshalsall](https://github.com/jameshalsall))
- Add integration tests [\#9](https://github.com/inviqa/kafka-consumer-go/pull/9) ([hgajjar](https://github.com/hgajjar))

## [v0.1.0](https://github.com/inviqa/kafka-consumer-go/tree/v0.1.0) (2021-06-21)

[Full Changelog](https://github.com/inviqa/kafka-consumer-go/compare/48d5d2dab678e327a3d9cfe87813f3d9ad665ef4...v0.1.0)

**Implemented enhancements:**

- Reduce the set up code for implementing services [\#2](https://github.com/inviqa/kafka-consumer-go/issues/2)

**Merged pull requests:**

- Expand GitHub actions to cover linting and gosec [\#7](https://github.com/inviqa/kafka-consumer-go/pull/7) ([jameshalsall](https://github.com/jameshalsall))
- Improve DX [\#3](https://github.com/inviqa/kafka-consumer-go/pull/3) ([hgajjar](https://github.com/hgajjar))
- Add GitHub actions checks [\#1](https://github.com/inviqa/kafka-consumer-go/pull/1) ([jameshalsall](https://github.com/jameshalsall))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
