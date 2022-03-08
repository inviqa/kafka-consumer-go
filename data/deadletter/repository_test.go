package deadletter

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewRepository(t *testing.T) {
	db, _, _ := sqlmock.New()
	exp := Repository{
		db: db,
	}

	if got := NewRepository(db); !reflect.DeepEqual(exp, got) {
		t.Errorf("expected %#v but got %#v", exp, got)
	}
}

func TestRepository_Count(t *testing.T) {
	db, mock, _ := sqlmock.New()
	ctx := context.Background()

	t.Run("it returns zero if there is a query error", func(t *testing.T) {
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM kafka_consumer_retries WHERE deadlettered.*`).
			WillReturnError(errors.New("oops"))

		repo := NewRepository(db)
		if got := repo.Count(ctx); got != 0 {
			t.Errorf("expected 0 count to be returned, but got %d", got)
		}
	})

	t.Run("it returns the dead-lettered count", func(t *testing.T) {
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM kafka_consumer_retries WHERE deadlettered.*`).
			WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(120))

		repo := NewRepository(db)
		if got := repo.Count(ctx); got != 120 {
			t.Errorf("expected 120 count to be returned, but got %d", got)
		}
	})
}
