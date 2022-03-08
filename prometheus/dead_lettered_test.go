package prometheus

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewDeadLetteredGauge(t *testing.T) {
	t.Run("it returns an error if DB connection fails", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected it to panic, but looks like it didn't")
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*2)
		ObserveDeadLetteredCount(ctx, erroringDbFactory{}, time.Second*1)
		cancel()
	})
}

func TestNewDeadLetteredGaugeWithRepo(t *testing.T) {
	t.Run("it returns a gauge that updates", func(t *testing.T) {
		repo := &mockRepository{count: 100}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*8)
		defer cancel()

		go observeDeadLetteredSizeUsingRepo(ctx, repo, time.Millisecond*5)
		time.Sleep(time.Millisecond * 8)
		if got := testutil.ToFloat64(deadLetteredCount); int(got) != 100 {
			t.Errorf("expected gauge to read 100, but got %d", int(got))
		}
	})
}

type erroringDbFactory struct {
}

func (e erroringDbFactory) DB() (*sql.DB, error) {
	return nil, errors.New("db connect error")
}

type mockRepository struct {
	count uint
}

func (m *mockRepository) Count(ctx context.Context) uint {
	return m.count
}
