//go:build integration
// +build integration

package integration

import (
	"testing"
	"time"

	"github.com/inviqa/kafka-consumer-go/integration/kafka"
)

func TestRegularDbMaintenance(t *testing.T) {
	defaultMaintenanceInterval := cfg.MaintenanceInterval
	cfg.MaintenanceInterval = time.Millisecond * 60
	defer func() {
		cfg.MaintenanceInterval = defaultMaintenanceInterval
	}()

	oneHourAgo := time.Now().In(time.UTC).Add(time.Hour * -1)
	oneMinuteAgo := time.Now().In(time.UTC).Add(time.Minute * -1)

	t.Run("it cleans up successfully processed retries updated over an hour ago", func(t *testing.T) {
		purgeDatabase()
		insertSuccessfullyProcessedDbRetry(oneHourAgo)
		insertSuccessfullyProcessedDbRetry(oneMinuteAgo)

		if got := retriesRecordCount(); got != 2 {
			t.Fatalf("expected 2 fixture records to be added to retry table, but there was %d instead", got)
		}

		// given the maintenance job is part of the consumer collection, we just start that
		// up as if under normal operation and wait until the job will have ran based on the
		// configured maintenance interval
		err := consumeFromKafkaUsingDbRetriesUntil(func(donech chan<- bool) {
			time.Sleep(time.Millisecond * 120)
			donech <- true
		}, kafka.NewTestConsumerHandler().Handle)

		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if got := retriesRecordCount(); got != 1 {
			t.Errorf("expected 1 retry record to remain, but got %d instead", got)
		}
	})

	t.Run("it does not touch errored and deadlettered retries", func(t *testing.T) {
		purgeDatabase()
		insertErroredProcessedDbRetry(oneHourAgo)
		insertDeadletteredProcessedDbRetry(oneHourAgo)

		if got := retriesRecordCount(); got != 2 {
			t.Fatalf("expected 2 fixture records to be added to retry table, but there was %d instead", got)
		}

		err := consumeFromKafkaUsingDbRetriesUntil(func(donech chan<- bool) {
			time.Sleep(time.Millisecond * 120)
			donech <- true
		}, kafka.NewTestConsumerHandler().Handle)

		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if got := retriesRecordCount(); got != 2 {
			t.Errorf("expected 2 retry records to remain, but got %d instead", got)
		}
	})
}
