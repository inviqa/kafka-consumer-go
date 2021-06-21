// +build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	consumer "github.com/inviqa/kafka-consumer-go"

	"github.com/inviqa/kafka-consumer-go/integration/kafka"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
)

const (
	testModeDocker = "docker"
)

var (
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
		log.Fatal(fmt.Sprintf("Failed to start Sarama producer: %s", err))
	}
}

func createConfig() *config.Config {
	args := os.Args
	defer func() {
		os.Args = args
	}()
	os.Args = nil
	os.Setenv("KAFKA_HOST", "localhost:9092")
	os.Setenv("KAFKA_GROUP", "test-kafka-consumer-go")
	os.Setenv("KAFKA_SOURCE_TOPICS", "mainTopic")
	os.Setenv("KAFKA_RETRY_INTERVALS", "1")

	c, _ := config.NewConfig()

	if os.Getenv("GO_TEST_MODE") == testModeDocker {
		c.Host = []string{"kafka:29092"}
	}

	return c
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

func consumeFromKafkaUntil(done func(chan<- bool), handler consumer.Handler) {
	doneCh := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	logger := consumer.NullLogger{}
	fch := make(chan consumer.Failure)
	handlerMap := consumer.HandlerMap{
		"mainTopic": handler,
	}

	producer, _ := consumer.NewFailureProducerWithDefaults(cfg, ctx, fch, logger)
	cons := consumer.NewCollection(cfg, producer, fch, handlerMap, config.NewSaramaConfig(false, false), logger)
	cons.Start(ctx, &sync.WaitGroup{})
	go done(doneCh)

	select {
	case <-time.After(10 * time.Second):
		break
	case <-doneCh:
		break
	}

	cancel()
	cons.Close()
}
