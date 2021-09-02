package consumer

import (
	"context"
	"errors"
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

		act1 := repo.getPublishedFailureByTopic("test")
		if diff := deep.Equal(f1, *act1); diff != nil {
			t.Error(diff)
		}
		act2 := repo.getPublishedFailureByTopic("test2")
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

type mockRetriesRepository struct {
	// indexed by topic name, as we only send 1 message per topic in our tests
	recvdFailures map[string]data.Failure
	willError     bool
}

func newMockRetriesRepository(willError bool) *mockRetriesRepository {
	return &mockRetriesRepository{
		recvdFailures: map[string]data.Failure{},
		willError:     willError,
	}
}

func (mr *mockRetriesRepository) PublishFailure(f data.Failure) error {
	if mr.willError {
		return errors.New("oops")
	}
	mr.recvdFailures[f.Topic] = f
	return nil
}

func (mr *mockRetriesRepository) getPublishedFailureByTopic(topic string) *data.Failure {
	f, ok := mr.recvdFailures[topic]
	if !ok {
		return nil
	}
	return &f
}
