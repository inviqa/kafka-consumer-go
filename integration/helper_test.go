//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"

	consumer "github.com/inviqa/kafka-consumer-go"
	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/integration/kafka"
	"github.com/inviqa/kafka-consumer-go/test"
)

var (
	db       *sql.DB
	cfg      *config.Config
	producer sarama.SyncProducer
)

func init() {
	cfg = createConfig()

	srmcfg := config.NewSaramaConfig(false, false)
	srmcfg.Producer.Return.Successes = true

	initKafkaProducer(srmcfg)

	var err error
	db, err = cfg.DB()
	if err != nil {
		log.Fatalf("failed to connect to the DB: %s", err)
	}

	if err = data.MigrateDatabase(db, cfg.DBSchema()); err != nil {
		log.Fatalf("failed to migrate the database: %s", err)
	}

	purgeDatabase()
}

func initKafkaProducer(srmcfg *sarama.Config) {
	var err error
	attempts := 10
	for {
		producer, err = sarama.NewSyncProducer(cfg.Host, srmcfg)
		attempts--
		if attempts == 0 {
			break
		}
		time.Sleep(time.Second * 1)
	}

	if producer == nil {
		log.Fatalf("failed to start Sarama producer: %s", err)
	}
}

func createConfig() *config.Config {
	c, err := config.NewBuilder().
		SetKafkaHost([]string{"localhost:9092"}).
		SetKafkaGroup("test").
		SetSourceTopics([]string{"mainTopic"}).
		SetRetryIntervals([]int{1}).
		SetDBHost("127.0.0.1").
		SetDBPass("kafka-consumer").
		SetDBUser("kafka-consumer").
		SetDBSchema("kafka-consumer").
		SetDBPort(15432).
		Config()

	if err != nil {
		panic(err)
	}

	return c
}

func purgeDatabase() {
	_, err := db.Exec("TRUNCATE TABLE kafka_consumer_retries;")
	if err != nil {
		panic(fmt.Sprintf("an error occurred cleaning the consumer retries table for tests: %s", err))
	}
}

func publishTestMessageToKafka(msg kafka.TestMessage) {
	b, _ := json.Marshal(msg)
	publishMessageToKafka(b, "mainTopic")
}

func publishMessageToKafka(b []byte, topic string) {
	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
		Key:   sarama.StringEncoder("message-key"),
		Headers: []sarama.RecordHeader{{
			Key:   []byte(`foo`),
			Value: []byte(`bar`),
		}},
	})
	if err != nil {
		log.Print("failed to send message to kafka:", err)
		return
	}
	log.Printf("published message to kafka partition %d offset %d", partition, offset)
}

func consumeFromKafkaUntil(done func(chan<- bool), handler consumer.Handler) error {
	return test.ConsumeFromKafkaUntil(cfg, consumer.HandlerMap{"mainTopic": handler}, time.Second*10, done)
}

func consumeFromKafkaUsingDbRetriesUntil(done func(chan<- bool), handler consumer.Handler) error {
	cfg.UseDBForRetryQueue = true
	defer func() {
		cfg.UseDBForRetryQueue = false
	}()

	return test.ConsumeFromKafkaUntil(cfg, consumer.HandlerMap{"mainTopic": handler}, time.Second*10, done)
}

func dbRetryWithEventId(eventId string) (*Retry, error) {
	row := db.QueryRow(
		`SELECT id, topic, payload_json, payload_headers, payload_key, kafka_offset, kafka_partition, attempts, deadlettered, successful, errored, last_error FROM kafka_consumer_retries WHERE payload_json::text LIKE $1`,
		fmt.Sprintf(`%%"event_id":"%s"%%`, eventId),
	)

	retry := &Retry{}
	err := row.Scan(&retry.ID, &retry.Topic, &retry.PayloadJSON, &retry.PayloadHeaders, &retry.PayloadKey, &retry.KafkaOffset, &retry.KafkaPartition, &retry.Attempts, &retry.Deadlettered, &retry.Successful, &retry.Errored, &retry.LastError)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("error trying to fetch db retry in integration tests: %w", err)
	}

	return retry, nil
}

func insertSuccessfullyProcessedDbRetry(updatedAt time.Time) {
	insertDbRetry(true, false, false, updatedAt)
}

func insertErroredProcessedDbRetry(updatedAt time.Time) {
	insertDbRetry(false, true, false, updatedAt)
}

func insertDeadletteredProcessedDbRetry(updatedAt time.Time) {
	insertDbRetry(false, false, true, updatedAt)
}

func insertDbRetry(successful, errored, deadlettered bool, updatedAt time.Time) {
	_, err := db.Exec(
		`INSERT INTO kafka_consumer_retries(topic, payload_json, payload_headers, kafka_offset, kafka_partition, payload_key, successful, errored, deadlettered, updated_at) VALUES('foo', '{}', '{}', 0, 0, '', $1, $2, $3, $4);`,
		successful,
		errored,
		deadlettered,
		updatedAt,
	)

	if err != nil {
		panic(err)
	}
}

func retriesRecordCount() int {
	var c int
	row := db.QueryRow(`SELECT COUNT(*) FROM kafka_consumer_retries;`)
	if err := row.Scan(&c); err != nil {
		panic(err)
	}
	return c
}
