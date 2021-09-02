package consumer

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries"
	"github.com/inviqa/kafka-consumer-go/log"
)

func Start(cfg *config.Config, ctx context.Context, hs HandlerMap, logger log.Logger) error {
	if logger == nil {
		logger = log.NullLogger{}
	}

	wg := &sync.WaitGroup{}
	fch := make(chan data.Failure)

	var producer failureProducer
	var err error
	if cfg.UseDBForRetryQueue {
		var db *sql.DB
		db, err = data.NewDB(cfg.GetDBConnectionString(), logger)
		if err != nil {
			return fmt.Errorf("could not start DB producer: %w", err)
		}

		retriesRepo := retries.NewRepository(cfg, db)

		producer = newDatabaseProducer(retriesRepo, fch, logger)
	} else {
		producer, err = newKafkaFailureProducerWithDefaults(cfg, fch, logger)
		if err != nil {
			return fmt.Errorf("could not start Kafka failure producer: %w", err)
		}
	}

	// todo: address DB polling in the consumer collection??
	cons := NewCollection(cfg, producer, fch, hs, config.NewSaramaConfig(cfg.TLSEnable, cfg.TLSSkipVerifyPeer), logger)

	if err = cons.Start(ctx, wg); err != nil {
		return fmt.Errorf("unable to start consumers: %w", err)
	}
	defer cons.Close()

	logger.Info("kafka consumer started")

	wg.Wait()

	return nil
}
