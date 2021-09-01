package consumer

import (
	"errors"
	"testing"

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
	repo := &mockRetriesRepository{}

	exp := &databaseProducer{
		repo:   repo,
		fch:    fch,
		logger: logger,
	}

	if diff := deep.Equal(exp, newDatabaseProducer(repo, fch, logger)); diff != nil {
		t.Error(diff)
	}
}

type mockRetriesRepository struct{}

func (mr *mockRetriesRepository) PublishFailure(f data.Failure) error {
	return errors.New("not implemented")
}
