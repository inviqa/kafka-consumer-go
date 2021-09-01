package consumer

import (
	"context"
	"sync"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/log"
)

type repository interface {
	PublishFailure(f data.Failure) error
}

// databaseProducer is a producer that listens for failures from kafka
// publish attempts and then sends them to the database for retry later
type databaseProducer struct {
	repo   repository
	fch    <-chan data.Failure
	logger log.Logger
}

func newDatabaseProducer(repo repository, fch <-chan data.Failure, logger log.Logger) failureProducer {
	return &databaseProducer{
		repo:   repo,
		fch:    fch,
		logger: logger,
	}
}

func (d databaseProducer) listenForFailures(ctx context.Context, wg *sync.WaitGroup) {
	panic("implement me")
}
