package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-test/deep"

	failuremodel "github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/data/retry/model"
)

func TestNewRepository(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	db, _, _ := sqlmock.New()
	exp := Repository{db: db}

	if diff := deep.Equal(exp, NewRepository(db)); diff != nil {
		t.Error(diff)
	}
}

func TestRepository_PublishFailure(t *testing.T) {
	db, mock, _ := sqlmock.New()
	repo := NewRepository(db)
	ctx := context.Background()
	f := failuremodel.Failure{
		Reason:         "something bad happened",
		Topic:          "product",
		NextTopic:      "retry1.payment.product",
		Message:        []byte(`{"foo":"bar"}`),
		MessageKey:     []byte(`SKU-123`),
		MessageHeaders: []byte(`{"buzz":"bazz"}`),
		KafkaPartition: 100,
		KafkaOffset:    200,
	}

	t.Run("failure successfully published to DB", func(t *testing.T) {
		mock.ExpectExec(`INSERT INTO kafka_consumer_retries.*`).
			WithArgs("product", []byte(`{"foo":"bar"}`), []byte(`{"buzz":"bazz"}`), 200, 100, "SKU-123").
			WillReturnResult(sqlmock.NewResult(1, 1))

		if err := repo.PublishFailure(ctx, f); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("error during insert", func(t *testing.T) {
		mock.ExpectExec(`INSERT INTO kafka_consumer_retries.*`).
			WillReturnError(errors.New("oops"))

		if err := repo.PublishFailure(ctx, f); err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func TestRepository_GetMessagesForRetry(t *testing.T) {
	db, mock, _ := sqlmock.New()
	repo := NewRepository(db)
	ctx := context.Background()

	t.Run("successfully fetches messages for retry", func(t *testing.T) {
		rows := sqlmock.NewRows(columns).
			AddRow(1, "product", `{"foo":"bar"}`, `{"buzz":"bar"}`, "foo", 100, 200, 1).
			AddRow(2, "product", `{"foo":"bazz"}`, "{}", "", 200, 300, 10)

		mock.ExpectExec("UPDATE kafka_consumer_retries.*").
			WithArgs(sqlmock.AnyArg(), "product", sqlmock.AnyArg(), 1, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 250))

		mock.ExpectQuery("SELECT .* FROM kafka_consumer_retries WHERE .*").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(rows)

		got, err := repo.GetMessagesForRetry(ctx, "product", 1, time.Second*10)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if diff := deep.Equal(expectedRetriesForTests(), got); diff != nil {
			t.Error(diff)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("error when creating batch is returned", func(t *testing.T) {
		expErr := errors.New("oops 1")
		mock.ExpectExec("UPDATE kafka_consumer_retries .*").
			WillReturnError(expErr)

		_, err := repo.GetMessagesForRetry(ctx, "product", 1, time.Second*10)
		if !errors.Is(err, expErr) {
			t.Errorf("expected error from update but got '%v'", err)
		}
	})

	t.Run("error when fetching batch is returned", func(t *testing.T) {
		expErr := errors.New("oops")

		mock.ExpectExec("UPDATE kafka_consumer_retries .*").
			WillReturnResult(sqlmock.NewResult(0, 250))

		mock.ExpectQuery("SELECT .* FROM kafka_consumer_retries WHERE .*").
			WillReturnError(expErr)

		_, err := repo.GetMessagesForRetry(ctx, "product", 1, time.Second*10)
		if !errors.Is(err, expErr) {
			t.Errorf("expected error from select but got '%v'", err)
		}
	})
}

func TestRepository_MarkRetryErrored(t *testing.T) {
	db, mock, _ := sqlmock.New()
	repo := NewRepository(db)
	ctx := context.Background()

	t.Run("retry marked as errored successfully", func(t *testing.T) {
		mock.ExpectExec("UPDATE kafka_consumer_retries SET .* WHERE .*").
			WithArgs(2, "something bad", true, false, 10).
			WillReturnResult(sqlmock.NewResult(1, 1))

		retry := model.Retry{
			ID:       10,
			Attempts: 2,
			Errored:  true,
		}

		if err := repo.MarkRetryErrored(ctx, retry, errors.New("something bad")); err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
	})

	t.Run("retry marked as deadlettered successfully", func(t *testing.T) {
		mock.ExpectExec("UPDATE kafka_consumer_retries SET .* WHERE .*").
			WithArgs(2, "something bad", true, true, 10).
			WillReturnResult(sqlmock.NewResult(1, 1))

		retry := model.Retry{
			ID:           10,
			Attempts:     2,
			Errored:      true,
			Deadlettered: true,
		}

		if err := repo.MarkRetryErrored(ctx, retry, errors.New("something bad")); err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
	})

	t.Run("error from database update is returned", func(t *testing.T) {
		mock.ExpectExec("UPDATE kafka_consumer_retries SET .* WHERE .*").
			WillReturnError(errors.New("oops"))

		retry := model.Retry{
			ID:       11,
			Attempts: 3,
		}

		if err := repo.MarkRetryErrored(ctx, retry, errors.New("something bad")); err == nil {
			t.Error("expected an error but got nil")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
	})
}

func TestRepository_MarkRetrySuccessful(t *testing.T) {
	db, mock, _ := sqlmock.New()
	repo := NewRepository(db)
	ctx := context.Background()

	t.Run("retry marked as successful successfully", func(t *testing.T) {
		mock.ExpectExec("UPDATE kafka_consumer_retries SET .* WHERE .*").
			WithArgs(3, 11).
			WillReturnResult(sqlmock.NewResult(1, 1))

		retry := model.Retry{
			ID:       11,
			Attempts: 3,
		}

		if err := repo.MarkRetrySuccessful(ctx, retry); err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
	})

	t.Run("error from database is returned", func(t *testing.T) {
		mock.ExpectExec("UPDATE kafka_consumer_retries SET .* WHERE .*").
			WillReturnError(errors.New("oops"))

		retry := model.Retry{
			ID:       12,
			Attempts: 4,
		}

		if err := repo.MarkRetrySuccessful(ctx, retry); err == nil {
			t.Error("expected an error but got nil")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
	})
}

func expectedRetriesForTests() []model.Retry {
	retry1 := model.Retry{
		ID:             1,
		Topic:          "product",
		PayloadJSON:    []byte(`{"foo":"bar"}`),
		PayloadHeaders: []byte(`{"buzz":"bar"}`),
		PayloadKey:     []byte("foo"),
		KafkaOffset:    100,
		KafkaPartition: 200,
		Attempts:       1,
	}
	retry2 := model.Retry{
		ID:             2,
		Topic:          "product",
		PayloadJSON:    []byte(`{"foo":"bazz"}`),
		PayloadHeaders: []byte(`{}`),
		PayloadKey:     []byte(""),
		KafkaOffset:    200,
		KafkaPartition: 300,
		Attempts:       10,
	}

	return []model.Retry{retry1, retry2}
}
