# Prometheus support

This module provides some basic gauges for Prometheus monitoring in your application. More may be added in the future.

## Using the gauges

Following [the Prometheus guide](https://prometheus.io/docs/guides/go-application/) on how to instrument your Go application, you can use these gauge observers to make things easier to set up. You only need to implement the HTTP server, following the guide, and then call the observe helpers provided by the `prometheus` package in this module. Of course, only call the observers that you care about.

>_NOTE: The observer helpers are blocking, so make sure you spawn them in a goroutine, and provide a `ctx` (`context.Context`) value tied to the shutdown of your application, so that the observers stop cleanly._

## The gauges

### Dead-lettered message count

This observer will update a gauge with the latest total number of dead-lettered messages in your database. Its name is `kafka_consumer_dead_lettered_count`.

>_NOTE: You must be using the [DB retries](/tools/docs/configuration.md#database-retries) feature to make use of this gauge._

### Example code

```go
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/prometheus"
)

func main() {
	ctx := context.Background()                   // use a suitable context
	kafkaCfg, err := config.NewBuilder().Config() // build the config as needed
	if err != nil {
		panic(err)
	}

	// this gauge will update every 30 seconds
	go prometheus.ObserveDeadLetteredCount(ctx, kafkaCfg, time.Second*30)

	http.Handle("/metrics", promhttp.Handler())
}
```