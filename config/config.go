package config

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/revdaalex/kafka-consumer-go/data"
)

var (
	defaultMaintenanceInterval = time.Hour * 1
)

type Config struct {
	Host             []string
	Group            string
	ConsumableTopics []*KafkaTopic
	TopicMap         map[TopicKey]*KafkaTopic
	// DBRetries is indexed by the topic name, and represents retry intervals for processing retries in the DB
	DBRetries           DBRetries
	TLSEnable           bool
	TLSSkipVerifyPeer   bool
	db                  Database
	UseDBForRetryQueue  bool
	MaintenanceInterval time.Duration
	topicNameGenerator  topicNameGenerator

	// memoized services
	services map[string]interface{}
}

type KafkaTopic struct {
	Name  string
	Delay time.Duration
	Key   TopicKey
	Next  *KafkaTopic
}

type Database struct {
	Host   string
	Port   int
	Schema string
	User   string
	Pass   string
	Driver string
}

type TopicKey string
type topicNameGenerator func(group, mainTopic, prefix string) string

func (cfg *Config) NextTopicNameInChain(currentTopic string) (string, error) {
	topic, ok := cfg.TopicMap[TopicKey(currentTopic)]
	if !ok {
		return "", fmt.Errorf("topic not found")
	}

	next := topic.Next

	if next == nil {
		return "", fmt.Errorf("there is no next topic in the chain")
	}

	return next.Name, nil
}

func (cfg *Config) NextTopicInChain(currentTopic string) (*KafkaTopic, error) {
	topic, ok := cfg.TopicMap[TopicKey(currentTopic)]
	if !ok {
		return nil, fmt.Errorf("topic not found")
	}

	next := topic.Next

	if next == nil {
		return nil, fmt.Errorf("there is no next topic in the chain")
	}

	return next, nil
}

func (cfg *Config) FindTopicKey(topicName string) TopicKey {
	topic, ok := cfg.TopicMap[TopicKey(topicName)]
	if !ok {
		return "default"
	}

	return topic.Key
}

// MainTopics will return a slice containing the main topic names from
// where messages are processed in Kafka. It will not include any of the
// retry or dead-letter topic names.
// This is only used in DB retries.
func (cfg *Config) MainTopics() []string {
	var mainTopics []string
	for _, t := range cfg.ConsumableTopics {
		if t.Delay == time.Duration(0) {
			mainTopics = append(mainTopics, t.Name)
		}
	}
	return mainTopics
}

// DB will connect to the database and return a *sql.DB value
// If a database connection already exists, it will return that instead of
// creating another one.
func (cfg *Config) DB() (*sql.DB, error) {
	if db, ok := cfg.services["db"]; ok {
		return db.(*sql.DB), nil
	}

	db, err := data.NewDB(cfg.dsn())
	cfg.services["db"] = db
	return db, err
}

func (cfg *Config) addTopicsFromSource(topics []string, retryIntervals []int) error {
	cfg.DBRetries = map[string][]*DBTopicRetry{}

	for _, topic := range topics {
		// main topic
		derivedTopics := []*KafkaTopic{
			{
				Name: topic,
				Key:  TopicKey(topic),
			},
		}
		cfg.DBRetries[topic] = []*DBTopicRetry{}

		// retry topics
		sequence := uint8(1)
		for i, interval := range retryIntervals {
			d, err := time.ParseDuration(strconv.Itoa(interval) + "s")
			if err != nil {
				return fmt.Errorf("consumer/config: could not parse delay in seconds %d in KAFKA_RETRY_INTERVALS env var: %v", interval, topics)
			}
			rt := &KafkaTopic{
				Name:  cfg.topicNameGenerator(cfg.Group, topic, fmt.Sprintf("retry%d", i+1)),
				Delay: d,
				Key:   TopicKey(topic),
			}

			dbRetry := &DBTopicRetry{
				Interval: d,
				Sequence: sequence,
				Key:      rt.Key,
			}

			derivedTopics[i].Next = rt
			derivedTopics = append(derivedTopics, rt)
			cfg.DBRetries[topic] = append(cfg.DBRetries[topic], dbRetry)
			sequence++
		}

		// deadLetter topic
		dt := &KafkaTopic{
			Name: cfg.topicNameGenerator(cfg.Group, topic, "deadLetter"),
			Key:  TopicKey(topic),
		}
		derivedTopics[len(derivedTopics)-1].Next = dt

		derivedTopics = append(derivedTopics, dt)

		cfg.addTopics(derivedTopics)
	}

	return nil
}

func (cfg *Config) dsn() string {
	sslMode := "disable"
	if cfg.TLSEnable {
		sslMode = "verify-full"
	}

	return fmt.Sprintf(
		"%s://%s@%s:%d/%s?sslmode=%s",
		cfg.db.Driver,
		url.UserPassword(cfg.db.User, cfg.db.Pass),
		cfg.db.Host,
		cfg.db.Port,
		cfg.db.Schema,
		sslMode,
	)
}

func (cfg *Config) addTopics(topics []*KafkaTopic) {
	cfg.ConsumableTopics = append(cfg.ConsumableTopics, topics[:len(topics)-1]...)

	if cfg.TopicMap == nil {
		cfg.TopicMap = map[TopicKey]*KafkaTopic{}
	}
	for _, topic := range topics {
		cfg.TopicMap[TopicKey(topic.Name)] = topic
	}
}

func (cfg *Config) loadFromBuilder(b *Builder) error {
	cfg.Host = b.kafkaHost
	cfg.Group = b.kafkaGroup
	cfg.TLSEnable = b.tlsEnable
	cfg.TLSSkipVerifyPeer = b.tlsSkipVerifyPeer
	cfg.UseDBForRetryQueue = b.useDbForRetries
	cfg.db.Host = b.dBHost
	cfg.db.User = b.dBUser
	cfg.db.Pass = b.dBPass
	cfg.db.Schema = b.dBSchema
	cfg.db.Port = b.dBPort
	cfg.db.Driver = b.dBDriver
	cfg.MaintenanceInterval = b.maintenanceInterval
	cfg.topicNameGenerator = b.topicNameGenerator

	retryIntervals := b.retryIntervals
	sourceTopics := b.sourceTopics
	if len(sourceTopics) == 0 {
		return errors.New("consumer/config: you must define some source topics")
	}

	if cfg.Host == nil || len(cfg.Host) == 0 {
		return errors.New("consumer/config: you must define a kafka host")
	}

	if strings.TrimSpace(cfg.Group) == "" {
		return errors.New("consumer/config: you must define a kafka group")
	}

	if err := cfg.addTopicsFromSource(sourceTopics, retryIntervals); err != nil {
		return fmt.Errorf("consumer/config: error loading config with topic names from builder: %w", err)
	}

	if cfg.MaintenanceInterval == 0 {
		cfg.MaintenanceInterval = defaultMaintenanceInterval
	}

	return nil
}

func (cfg *Config) DBSchema() string {
	return cfg.db.Schema
}
