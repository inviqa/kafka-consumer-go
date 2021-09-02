package consumer

import (
	"context"
	"sync"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/log"
)

type producerRepository interface {
	PublishFailure(f data.Failure) error
}

// databaseProducer is a producer that listens for failed push attempts to kafka
// sent on fch and then sends them to the database for retry later
type databaseProducer struct {
	repo   producerRepository
	fch    <-chan data.Failure
	logger log.Logger
}

func newDatabaseProducer(repo producerRepository, fch <-chan data.Failure, logger log.Logger) failureProducer {
	return &databaseProducer{
		repo:   repo,
		fch:    fch,
		logger: logger,
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
				// TODO: move the publishing into a separate goroutine to increase throughput?
				if err := d.repo.PublishFailure(f); err != nil {
					d.logger.Errorf("error publishing a failure to database for retry: %s", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
