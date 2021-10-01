package consumer

import (
	"context"
	"sync"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/log"
)

// databaseProducer is a producer that listens for failed push attempts to kafka
// sent on fch and then sends them to the database for retry later
type databaseProducer struct {
	retryManager retryManager
	fch          <-chan data.Failure
	logger       log.Logger
}

func newDatabaseProducer(rm retryManager, fch <-chan data.Failure, logger log.Logger) *databaseProducer {
	return &databaseProducer{
		retryManager: rm,
		fch:          fch,
		logger:       logger,
	}
}

func (d databaseProducer) listenForFailures(ctx context.Context, wg *sync.WaitGroup) {
	d.logger.Info("starting database retry producer")

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case f := <-d.fch:
				if err := d.retryManager.PublishFailure(context.Background(), f); err != nil {
					d.logger.Errorf("error publishing a failure to database for retry: %s", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
