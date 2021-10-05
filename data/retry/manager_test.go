package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data/failure"
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
		manager, repo := newManagerForTests(false)
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
		manager, _ := newManagerForTests(true)

		_, err := manager.GetBatch(context.Background(), "foo", 1, time.Second*1)
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func TestManager_MarkSuccessful(t *testing.T) {
	ctx := context.Background()

	t.Run("marks retry successful", func(t *testing.T) {
		manager, repo := newManagerForTests(false)
		retry := model.Retry{ID: 123}
		if err := manager.MarkSuccessful(ctx, retry); err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		expRetry := model.Retry{
			ID:       123,
			Attempts: 1,
		}

		if diff := deep.Equal(&expRetry, repo.RetryMarkedSuccessful); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("previously errored retry is marked successful", func(t *testing.T) {
		manager, repo := newManagerForTests(false)
		retry := model.Retry{
			ID:      123,
			Errored: true,
		}
		if err := manager.MarkSuccessful(ctx, retry); err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		expRetry := model.Retry{
			ID:       123,
			Attempts: 1,
		}

		if diff := deep.Equal(&expRetry, repo.RetryMarkedSuccessful); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("returns error from repository", func(t *testing.T) {
		manager, _ := newManagerForTests(true)

		if err := manager.MarkSuccessful(ctx, model.Retry{}); err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func TestManager_MarkErrored(t *testing.T) {
	ctx := context.Background()

	t.Run("marks retry errored", func(t *testing.T) {
		manager, repo := newManagerForTests(false)
		retry := model.Retry{
			ID:    123,
			Topic: "foo",
		}
		err := manager.MarkErrored(ctx, retry, errors.New("foo"))
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		expRetry := model.Retry{
			ID:       123,
			Topic:    "foo",
			Errored:  true,
			Attempts: 1,
		}

		if diff := deep.Equal(&expRetry, repo.RetryMarkedErrored); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("marks retry deadlettered when no more retries needed", func(t *testing.T) {
		manager, repo := newManagerForTests(false)
		retry := model.Retry{
			ID:       123,
			Topic:    "foo",
			Attempts: 1,
		}
		err := manager.MarkErrored(ctx, retry, errors.New("foo"))
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		expRetry := model.Retry{
			ID:           123,
			Topic:        "foo",
			Errored:      true,
			Deadlettered: true,
			Attempts:     2,
		}

		if diff := deep.Equal(&expRetry, repo.RetryMarkedErrored); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("returns error from repository", func(t *testing.T) {
		manager, _ := newManagerForTests(true)

		err := manager.MarkErrored(ctx, model.Retry{}, errors.New("oops"))
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func TestManager_PublishFailure(t *testing.T) {
	ctx := context.Background()

	t.Run("publishes failure", func(t *testing.T) {
		manager, repo := newManagerForTests(false)
		f := failure.Failure{
			Topic: "foo",
		}

		if err := manager.PublishFailure(ctx, f); err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if diff := deep.Equal(&f, repo.PublishedFailure); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("returns error from repository", func(t *testing.T) {
		manager, _ := newManagerForTests(true)

		if err := manager.PublishFailure(ctx, failure.Failure{}); err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func newManagerForTests(repoWillError bool) (Manager, *mockRepository) {
	repo := newMockRepository(repoWillError)
	manager := Manager{
		dbRetries: dummyDbRetriesForManagerTests(),
		repo:      repo,
	}
	return manager, repo
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
