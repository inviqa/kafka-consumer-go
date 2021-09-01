package retries

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-test/deep"
)

func TestNewRepository(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	db, _, _ := sqlmock.New()
	exp := Repository{
		db: db,
	}

	if diff := deep.Equal(exp, NewRepository(db)); diff != nil {
		t.Error(diff)
	}
}
