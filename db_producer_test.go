package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/log"
)

func TestNewDatabaseProducer(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	fch := make(chan data.Failure)
	logger := log.NullLogger{}
	repo := newMockRetriesRepository(false)

	exp := &databaseProducer{
		repo:   repo,
		fch:    fch,
		logger: logger,
	}

	if diff := deep.Equal(exp, newDatabaseProducer(repo, fch, logger)); diff != nil {
		t.Error(diff)
	}
}

func TestDatabaseProducer_ListenForFailures(t *testing.T) {
	f1 := data.Failure{
		Reason:  "something bad happened",
		Message: []byte("hello"),
		Topic:   "test",
	}
	f2 := data.Failure{
		Reason:  "something bad happened",
		Message: []byte("world"),
		Topic:   "test2",
	}

	t.Run("failure is pushed to repository", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		repo := newMockRetriesRepository(false)
		fch := make(chan data.Failure, 1)

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
		repo := newMockRetriesRepository(true)
		fch := make(chan data.Failure, 1)

		newDatabaseProducer(repo, fch, log.NullLogger{}).listenForFailures(ctx, &sync.WaitGroup{})
		fch <- f1
		fch <- f2
		time.Sleep(time.Millisecond * 5)
		cancel()
	})
}
