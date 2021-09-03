package retries

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
)

func TestNewRepository(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	db, _, _ := sqlmock.New()
	cfg := &config.Config{}
	exp := Repository{
		db:  db,
		cfg: cfg,
	}

	if diff := deep.Equal(exp, NewRepository(cfg, db)); diff != nil {
		t.Error(diff)
	}
}

func TestRepository_PublishFailure(t *testing.T) {
	db, mock, _ := sqlmock.New()
	repo := NewRepository(&config.Config{}, db)
	f := data.Failure{
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
			WithArgs("product", []byte(`{"foo":"bar"}`), []byte(`{"buzz":"bazz"}`), 200, 100, []byte("SKU-123")).
			WillReturnResult(sqlmock.NewResult(1, 1))

		if err := repo.PublishFailure(f); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("error during insert", func(t *testing.T) {
		mock.ExpectExec(`INSERT INTO kafka_consumer_retries.*`).
			WillReturnError(errors.New("oops"))

		if err := repo.PublishFailure(f); err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func TestRepository_GetMessagesForRetry(t *testing.T) {
	db, mock, _ := sqlmock.New()
	repo := NewRepository(&config.Config{}, db)

	t.Run("successfully fetches messages for retry", func(t *testing.T) {
		times := exampleCreatedUpdatedTimes()
		rows := sqlmock.NewRows(columns).
			AddRow(1, "product", `{"foo":"bar"}`, `{"buzz":"bar"}`, "foo", 100, 200, 1, "", times["created"], times["updated"]).
			AddRow(2, "product", `{"foo":"bazz"}`, "{}", "", 200, 300, 10, "something bad", times["created"], times["updated"])

		mock.ExpectQuery("SELECT .* FROM kafka_consumer_retries WHERE .*").
			WithArgs("product", 1, sqlmock.AnyArg()).
			WillReturnRows(rows)

		got, err := repo.GetMessagesForRetry("product", 1, time.Second*10)
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

	t.Run("error from database is returned", func(t *testing.T) {
		mock.ExpectQuery("SELECT .* FROM kafka_consumer_retries WHERE .*").
			WillReturnError(errors.New("oops"))

		_, err := repo.GetMessagesForRetry("product", 1, time.Second*10)
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func expectedRetriesForTests() []Retry {
	times := exampleCreatedUpdatedTimes()

	retry1 := Retry{
		ID:             1,
		Topic:          "product",
		PayloadJSON:    []byte(`{"foo":"bar"}`),
		PayloadHeaders: []byte(`{"buzz":"bar"}`),
		PayloadKey:     []byte("foo"),
		KafkaOffset:    100,
		KafkaPartition: 200,
		Attempts:       1,
		CreatedAt:      times["created"],
		UpdatedAt:      times["updated"],
	}
	retry2 := Retry{
		ID:             2,
		Topic:          "product",
		PayloadJSON:    []byte(`{"foo":"bazz"}`),
		PayloadHeaders: []byte(`{}`),
		PayloadKey:     []byte(""),
		KafkaOffset:    200,
		KafkaPartition: 300,
		Attempts:       10,
		LastError:      "something bad",
		CreatedAt:      times["created"],
		UpdatedAt:      times["updated"],
	}

	return []Retry{retry1, retry2}
}

func exampleCreatedUpdatedTimes() map[string]time.Time {
	createdAt, _ := time.Parse(time.RFC3339, "2010-01-01T10:00:00Z")
	updatedAt, _ := time.Parse(time.RFC3339, "2010-01-01T11:00:00Z")

	return map[string]time.Time{
		"created": createdAt,
		"updated": updatedAt,
	}
}
