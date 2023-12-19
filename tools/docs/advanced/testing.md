# Testing

You will likely want to write integration tests for your consumer service. This document explains how to do this.

## Configuration

Configuration for this module, as described in its [documentation](/tools/docs/configuration.md), is created using the config builder. However, this module provides a convenience method for creating configuration in your tests, where you can provide raw values for all options rather than setting env vars:

```go
package integration

import (
	kctest "github.com/revdaalex/kafka-consumer-go/test"
)

func init() {
	consumerCfg, err := kctest.NewConfig("localhost:9092", "testGroup", []string{"test.order,test.payment"}, []int{1, 2})
	if err != nil {
		panic(err)
	}

	// ...
}
```

## Consuming

In your integration tests, you will want to run the consumer and wait until any messages have been processed. This module provides a convenience function for doing this in its `test` package.

This example shows how an integration test might look for a common type of consumer: one that forwards on messages from Kafka as HTTP requests to a 3rd party REST API. Generally, we may approach this by using a [`httptest` server](https://pkg.go.dev/net/http/httptest#example-Server) to record requests that our app sends, publishing test Kafka messages to the broker, and then running the consumer until they have been pushed to the test server as expected.

```go
package integration

import (
	"testing"
	"time"
	
	"github.com/revdaalex/kafka-consumer-go"
	kctest "github.com/revdaalex/kafka-consumer-go/test"
)

func TestSomething(t *testing.T) {
	// 1. set up any of your own components, like your httptest server to mock your 3rd party API
	// 2. publish some message to Kafka
	// 3. consume from Kafka until some condition becomes true:
	// 4. run final assertion on the condition
	
	handlerMap := consumer.HandlerMap{"test.order": MyOrderHandler} 
	
	kctest.ConsumeFromKafkaUntil(consumerCfg, handlerMap, time.Second * 5, func(donech chan<- bool) {
		for {
			if myHttpTestServer.receivedCount == 1 {
				donech <- true
			}
		}
	})
	
	if myHttpTestServer.receivedCount != 1 {
		t.Errorf("expected http client to receive 1 request, but got %d instead", myHttpTestServer.receivedCount)
	}
}
```

>_NOTE: The reason we do a final check on the condition is because in the event of a timeout, i.e. more than 5 seconds elapses and the condition has not become true in our done function on line 49, the test will proceed past line 49 and would pass incorrectly. The final assertion on line 55 confirms that everything worked as expected._