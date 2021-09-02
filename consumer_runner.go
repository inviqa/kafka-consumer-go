package consumer

import (
	"context"
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
	srmCfg := config.NewSaramaConfig(cfg.TLSEnable, cfg.TLSSkipVerifyPeer)

	var producer failureProducer
	var cons ConsumerCollection
	var err error

	if cfg.UseDBForRetryQueue {
		db, err := data.NewDB(cfg.GetDBConnectionString(), logger)
		if err != nil {
			return fmt.Errorf("could not connect to DB: %w", err)
		}
		repo := retries.NewRepository(cfg, db)
		producer = newDatabaseProducer(repo, fch, logger)
		cons = newKafkaConsumerDbCollection(cfg, producer, repo, fch, hs, srmCfg, logger)
	} else {
		producer, err = newKafkaFailureProducerWithDefaults(cfg, fch, logger)
		if err != nil {
			return fmt.Errorf("could not start Kafka failure producer: %w", err)
		}
		cons = newKafkaConsumerCollection(cfg, producer, fch, hs, srmCfg, logger)
	}

	if err = cons.Start(ctx, wg); err != nil {
		return fmt.Errorf("unable to start consumers: %w", err)
	}
	defer cons.Close()

	logger.Info("kafka consumer started")

	wg.Wait()

	return nil
}
