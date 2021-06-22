// +build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"

	consumer "github.com/inviqa/kafka-consumer-go"
	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/integration/kafka"
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
	os.Setenv("KAFKA_HOST", "localhost:9092")
	os.Setenv("KAFKA_GROUP", "test")
	os.Setenv("KAFKA_SOURCE_TOPICS", "mainTopic")
	os.Setenv("KAFKA_RETRY_INTERVALS", "1")

	c, err := config.NewConfig()
	if err != nil {
		panic(err)
	}

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	fch := make(chan consumer.Failure)
	handlerMap := consumer.HandlerMap{
		"mainTopic": handler,
	}

	go func() {
		defer cancel()
		select {
		case <-doneCh:
			return
		case <-ctx.Done():
			return
		}
	}()

	go done(doneCh)
	consumer.Start(cfg, ctx, fch, handlerMap, nil)
}
