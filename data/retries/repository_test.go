package retries

import (
	"errors"
	"testing"

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
