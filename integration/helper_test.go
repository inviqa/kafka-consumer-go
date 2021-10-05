// +build integration

package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"

	consumer "github.com/inviqa/kafka-consumer-go"
	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/integration/kafka"
	ourlog "github.com/inviqa/kafka-consumer-go/log"
	"github.com/inviqa/kafka-consumer-go/test"
)

const (
	testModeDocker = "docker"
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
	var err error
	producer, err = sarama.NewSyncProducer(cfg.Host, srmcfg)
	if err != nil {
		log.Fatalf("failed to start Sarama producer: %s", err)
	}

	db, err = data.NewDB(cfg.GetDBConnectionString(), ourlog.NullLogger{})
	if err != nil {
		log.Fatalf("failed to connect to the DB: %s", err)
	}

	purgeDatabase()

	if err = data.MigrateDatabase(db, cfg); err != nil {
		log.Fatalf("failed to migrate the database: %s", err)
	}
}

func createConfig() *config.Config {
	os.Setenv("KAFKA_HOST", "localhost:9092")
	os.Setenv("KAFKA_GROUP", "test")
	os.Setenv("KAFKA_SOURCE_TOPICS", "mainTopic")
	os.Setenv("KAFKA_RETRY_INTERVALS", "1")
	os.Setenv("DB_HOST", "127.0.0.1")
	os.Setenv("DB_PORT", "15432")
	os.Setenv("DB_USER", "kafka-consumer")
	os.Setenv("DB_PASS", "kafka-consumer")
	os.Setenv("DB_SCHEMA", "kafka-consumer")

	c, err := config.NewConfig()
	if err != nil {
		panic(err)
	}

	if os.Getenv("GO_TEST_MODE") == testModeDocker {
		c.Host = []string{"kafka:29092"}
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
	})
	if err != nil {
		log.Print("failed to send message to kafka:", err)
		return
	}
	log.Printf("published message to kafka partition %d offset %d", partition, offset)
}

func consumeFromKafkaUntil(done func(chan<- bool), handler consumer.Handler) error {
	return test.ConsumeFromKafkaUntil(cfg, consumer.HandlerMap{"mainTopic": handler}, time.Second*5, done)
}

func consumeFromKafkaUsingDbRetriesUntil(done func(chan<- bool), handler consumer.Handler) error {
	cfg.UseDBForRetryQueue = true
	defer func() {
		cfg.UseDBForRetryQueue = false
	}()

	return test.ConsumeFromKafkaUntil(cfg, consumer.HandlerMap{"mainTopic": handler}, time.Second*10, done)
}
