package consumer

import (
	"context"
	"database/sql"
	"sync"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/retries"
	"github.com/inviqa/kafka-consumer-go/log"
)

func Start(cfg *config.Config, ctx context.Context, hs HandlerMap, logger log.Logger) {
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
			// todo: return error instead
			logger.Panic("could not start DB producer")
		}

		retriesRepo := retries.NewRepository(db)

		producer = newDatabaseProducer(retriesRepo, fch, logger)
	} else {
		producer, err = newKafkaFailureProducerWithDefaults(cfg, fch, logger)
		if err != nil {
			// todo: return error instead
			logger.Panic("could not start Kafka failure producer")
		}
	}

	// todo: address DB polling in the consumer collection??
	cons := NewCollection(cfg, producer, fch, hs, config.NewSaramaConfig(cfg.TLSEnable, cfg.TLSSkipVerifyPeer), logger)

	if err = cons.Start(ctx, wg); err != nil {
		// todo: return error instead
		logger.Panic("unable to start consumers")
	}
	defer cons.Close()

	logger.Info("kafka consumer started")

	wg.Wait()
}
