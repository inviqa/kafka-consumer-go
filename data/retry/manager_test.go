package retry

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data/retry/internal"
	"github.com/inviqa/kafka-consumer-go/data/retry/model"
)

func TestNewManagerWithDefaults(t *testing.T) {
	db, _, _ := sqlmock.New()
	dbRetries := config.DBRetries{}

	exp := &Manager{
		dbRetries: dbRetries,
		repo:      internal.NewRepository(db),
	}

	got := NewManagerWithDefaults(dbRetries, db)
	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

func TestManager_GetBatch(t *testing.T) {
	t.Run("returns batch from repository", func(t *testing.T) {
		repo := newMockRepository(false)
		manager := Manager{
			dbRetries: dummyDbRetriesForManagerTests(),
			repo:      repo,
		}

		expRetries := []model.Retry{
			{
				ID:    10,
				Topic: "foo",
			},
		}
		repo.retriesToReturn = expRetries

		got, err := manager.GetBatch(context.Background(), "foo", 1, time.Second*1)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if diff := deep.Equal(expRetries, got); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("returns error from repository", func(t *testing.T) {
		repo := newMockRepository(true)
		manager := Manager{
			dbRetries: dummyDbRetriesForManagerTests(),
			repo:      repo,
		}

		_, err := manager.GetBatch(context.Background(), "foo", 1, time.Second*1)
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func TestManager_MarkSuccessful(t *testing.T) {
	t.Skip()
}

func TestManager_MarkErrored(t *testing.T) {
	t.Skip()
}

func TestManager_PublishFailure(t *testing.T) {
	t.Skip()
}

func dummyDbRetriesForManagerTests() config.DBRetries {
	return config.DBRetries{
		"foo": {
			{
				Interval: time.Second * 1,
				Sequence: 1,
				Key:      "foo",
			},
			{
				Interval: time.Second * 2,
				Sequence: 2,
				Key:      "foo",
			},
		},
	}
}
