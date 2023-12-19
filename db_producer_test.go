package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/revdaalex/kafka-consumer-go/data/failure/model"
	"github.com/revdaalex/kafka-consumer-go/log"
)

func TestNewDatabaseProducer(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	fch := make(chan model.Failure)
	logger := log.NullLogger{}
	rm := newMockRetryManager(false)

	exp := &databaseProducer{
		retryManager: rm,
		fch:          fch,
		logger:       logger,
	}

	if diff := deep.Equal(exp, newDatabaseProducer(rm, fch, logger)); diff != nil {
		t.Error(diff)
	}
}

func TestDatabaseProducer_ListenForFailures(t *testing.T) {
	f1 := model.Failure{
		Reason:  "something bad happened",
		Message: []byte("hello"),
		Topic:   "test",
	}
	f2 := model.Failure{
		Reason:  "something bad happened",
		Message: []byte("world"),
		Topic:   "test2",
	}

	t.Run("failure is pushed to repository", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		repo := newMockRetryManager(false)
		fch := make(chan model.Failure, 1)

		newDatabaseProducer(repo, fch, log.NullLogger{}).listenForFailures(ctx, &sync.WaitGroup{})
		fch <- f1
		fch <- f2
		time.Sleep(time.Millisecond * 5)
		cancel()

		act1 := repo.getFirstPublishedFailureByTopic("test")
		if diff := deep.Equal(f1, *act1); diff != nil {
			t.Error(diff)
		}
		act2 := repo.getFirstPublishedFailureByTopic("test2")
		if diff := deep.Equal(f2, *act2); diff != nil {
			t.Error(diff)
		}
	})

	// we run this test purely to ensure that an error scenario does not
	// cause a panic
	t.Run("error publishing failure to DB", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		repo := newMockRetryManager(true)
		fch := make(chan model.Failure, 1)

		newDatabaseProducer(repo, fch, log.NullLogger{}).listenForFailures(ctx, &sync.WaitGroup{})
		fch <- f1
		fch <- f2
		time.Sleep(time.Millisecond * 5)
		cancel()
	})
}
