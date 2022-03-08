package prometheus

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/inviqa/kafka-consumer-go/data/deadletter"
)

type dbFactory interface {
	DB() (*sql.DB, error)
}

type deadLetteredRepo interface {
	Count(ctx context.Context) uint
}

var deadLetteredCount prom.Gauge

func ObserveDeadLetteredCount(ctx context.Context, dbf dbFactory, refreshInterval time.Duration) {
	db, err := dbf.DB()
	if err != nil {
		panic(fmt.Errorf("prometheus: error connecting to database: %s", err))
	}

	repo := deadletter.NewRepository(db)
	observeDeadLetteredSizeUsingRepo(ctx, repo, refreshInterval)
}

func observeDeadLetteredSizeUsingRepo(ctx context.Context, repo deadLetteredRepo, refreshInterval time.Duration) {
	deadLetteredCount = promauto.NewGauge(prom.GaugeOpts{
		Name: "kafka_consumer_dead_lettered_count",
		Help: "The number of messages that are marked as dead-lettered.",
	})

	monitorDeadLettered(ctx, repo, refreshInterval)
}

func monitorDeadLettered(ctx context.Context, repo deadLetteredRepo, refreshInterval time.Duration) {
	for {
		count := repo.Count(ctx)

		select {
		case _ = <-ctx.Done():
			return
		default:
			deadLetteredCount.Set(float64(count))
			time.Sleep(refreshInterval)
		}
	}
}
