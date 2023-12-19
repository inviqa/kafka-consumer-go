# Configuration

To use this module you must provide valid configuration to it. This is done using the `config.Builder{}` type (instantiated via `config.NewBuilder()`), and then using the various setters on it to configure the consumer, before finally calling `.Config()` to get a `config.Config{}` value.

## Configuration options

| Name                 | Type            | Required? | Description                                                                                                                                                                                                                             |
|----------------------|-----------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Kafka host           | `[]string`      | Yes       | The Kafka broker(s) to consume from. Multiple brokers should be separated by a comma.                                                                                                                                                   |
| Kafka group          | `string`        | Yes       | The Kafka group name for your consumer.                                                                                                                                                                                                 |
| Source topics        | `[]string`      | Yes       | The topics to consume messages from.                                                                                                                                                                                                    |
| Retry intervals      | `[]int`         | No        | The intervals, in seconds, of the retries in your retry chain. See [Kafka topics](#kafka-topics) for more info. If this is omitted then no retries will be attempted for messages.                                                      |
| Use DB for retries   | `bool`          | No        | Whether to store messages that need retrying in the database. If false, then messages that need retrying will be stored in Kafka topics instead. See  [Kafka topics](#kafka-topics). **Defaults to false**.                             |
| DB host              | `string`        | No        | The database host where the outbox table resides. NOTE: This is required if you enable database-based retries.                                                                                                                          |
| DB port              | `int`           | No        | Database port. **Defaults to 5432**.                                                                                                                                                                                                    |
| DB user              | `string`        | No        | Database user.                                                                                                                                                                                                                          |
| DB pass              | `string`        | No        | Database password.                                                                                                                                                                                                                      |
| DB schema            | `string`        | No        | Database name.                                                                                                                                                                                                                          |
| Maintenance interval | `time.Duration` | No        | How regularly the maintenance job will be run. **Defaults to every hour**. NOTE: You do not need to worry about this if you are not using [database retries](#database-retries). Even then, you should never need to change this value. |
| TLS enable           | `bool`          | No        | Whether to enable TLS when communicating with Kafka and the database. We recommend enabling this if your database and Kafka cluster support it. **Defaults to false.**                                                                  |
| TLS skip verify peer | `bool`          | No        | Whether to skip peer verification when connecting over TLS. **Defaults to false.**                                                                                                                                                      |

### Example of builder

```go

package main

import (
	"time"

	"github.com/revdaalex/kafka-consumer-go/config"
)

func main() {
	consumerCfg, err := config.NewBuilder().
		SetKafkaHost([]string{"broker1", "broker2"}).
		SetKafkaGroup("group").
		SetSourceTopics([]string{"product"}).
		SetRetryIntervals([]int{120}).
		SetDBHost("postgres").
		SetDBPass("pass").
		SetDBUser("user").
		SetDBSchema("schema").
		UseDbForRetries(true).
		Config()
	
	if err != nil {
		panic(err)
	}
	
	// ...
}

```

## Kafka topics

You can use the "Kafka source topics" and "Kafka retry topics" configuration values to control which topics to consume from in your cluster. This module generates a chain of topics with retry intervals based on the provided configuration.

For example, the following config:

```go
consumerCfg, err := config.NewBuilder().
		SetKafkaHost([]string{"broker1"}).
		SetKafkaGroup("algolia").
		SetSourceTopics([]string{"product"}).
		SetRetryIntervals([]int{120}).
		Config()
```

would generate a topic chain of

`product` -> `retry1.algolia.product` (delay of 120 secs) -> `deadLetter.algolia.product`

You can see it has automatically generated the retry and deadLetter topic names along with the retry delay.

>_NOTE: You do not need to have any retry topics in the chain, but it is advisable in most circumstances. If you don't set any retry intervals, then it would directly send the failures to the deadLetter topic._

### Database retries

If you use `UseDbForRetries(true)` in your config builder, then messages needing a retry will be stored in a Postgres database table that is automatically created when the consumer starts. You will need to provide database credentials using the `SetDb*()` builder setters.

>_NOTE: We may add support for additional database engines in a future release._

### Flow of event processing:

Sticking the configuration example above, this will tell this module to:

* Consume records from `product`
* If there are errors during the processing of those records then publish them to the next topic in the chain: `retry1.algolia.product` (or the database if you have enabled DB retries).
* Wait for 120 seconds before processing the errored messages again
* If there are any errors processing these messages, then publish them to the last topic in the chain: `deadLetter.algolia.product` (or mark them as dead-lettered in the database table).

> _NOTE: Messages that are dead-lettered will not be processed again, as these messages have usually failed multiple times and more retries are unlikely to resolve the situation. They will usually need manual intervention._

### Multiple sets of topics

See [using multiple main topics](advanced/using-multiple-main-topics.md).
