package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	EnvVarHost                  = "KAFKA_HOST"
	EnvVarGroup                 = "KAFKA_GROUP"
	EnvVarSourceTopics          = "KAFKA_SOURCE_TOPICS"
	EnvVarRetryIntervals        = "KAFKA_RETRY_INTERVALS"
	EnvVarTLSEnable             = "TLS_ENABLE"
	EnvVarTLSSkipVerify         = "TLS_SKIP_VERIFY_PEER"
	EnvVarDbRetryQueue          = "USE_DB_RETRY_QUEUE"
	EnvVarDbMaintenanceInterval = "MAINTENANCE_INTERVAL_SECONDS"
	EnvVarDbHost                = "DB_HOST"
	EnvVarDbPort                = "DB_PORT"
	EnvVarDbPass                = "DB_PASS"
	EnvVarDbUser                = "DB_USER"
	EnvVarDbSchema              = "DB_SCHEMA"
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
	DB                  Database
	UseDBForRetryQueue  bool
	MaintenanceInterval time.Duration
	topicNameGenerator  topicNameGenerator
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
}

type TopicKey string
type topicNameGenerator func(group, mainTopic, prefix string) string

func NewConfig() (*Config, error) {
	return buildConfig(Config{
		topicNameGenerator: defaultTopicNameGenerator,
	})
}

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

func (cfg *Config) FindTopicKey(topicName string) TopicKey {
	topic, ok := cfg.TopicMap[TopicKey(topicName)]
	if !ok {
		return "default"
	}

	return topic.Key
}

func (cfg *Config) GetDBConnectionString() string {
	sslMode := "disable"
	if cfg.TLSEnable {
		sslMode = "verify-full"
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", cfg.DB.User, cfg.DB.Pass, cfg.DB.Host, cfg.DB.Port, cfg.DB.Schema, sslMode)
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

func (cfg *Config) addTopics(topics []*KafkaTopic) {
	cfg.ConsumableTopics = append(cfg.ConsumableTopics, topics[:len(topics)-1]...)

	if cfg.TopicMap == nil {
		cfg.TopicMap = map[TopicKey]*KafkaTopic{}
	}
	for _, topic := range topics {
		cfg.TopicMap[TopicKey(topic.Name)] = topic
	}
}

func (cfg *Config) loadFromEnvVars() error {
	cfg.Host = envVarAsStringSlice(EnvVarHost)
	cfg.Group = os.Getenv(EnvVarGroup)
	cfg.TLSEnable = envVarAsBool(EnvVarTLSEnable)
	cfg.TLSSkipVerifyPeer = envVarAsBool(EnvVarTLSSkipVerify)
	cfg.UseDBForRetryQueue = envVarAsBool(EnvVarDbRetryQueue)
	cfg.DB.Host = os.Getenv(EnvVarDbHost)
	cfg.DB.User = os.Getenv(EnvVarDbUser)
	cfg.DB.Pass = os.Getenv(EnvVarDbPass)
	cfg.DB.Schema = os.Getenv(EnvVarDbSchema)
	cfg.DB.Port = envVarAsInt(EnvVarDbPort)
	cfg.MaintenanceInterval = time.Second * time.Duration(envVarAsInt(EnvVarDbMaintenanceInterval))

	sourceTopics := envVarAsStringSlice(EnvVarSourceTopics)
	if len(sourceTopics) == 0 {
		return fmt.Errorf("consumer/config: you must define a %s value", EnvVarSourceTopics)
	}

	retryIntervals, err := envVarAsIntSlice(EnvVarRetryIntervals)
	if err != nil {
		return fmt.Errorf("consumer/config: error parsing %s: %w", EnvVarRetryIntervals, err)
	}

	if cfg.Host == nil || len(cfg.Host) == 0 {
		return fmt.Errorf("consumer/config: you must define a %s value", EnvVarHost)
	}

	if strings.TrimSpace(cfg.Group) == "" {
		return fmt.Errorf("consumer/config: you must define a %s value", EnvVarGroup)
	}

	if err := cfg.addTopicsFromSource(sourceTopics, retryIntervals); err != nil {
		return fmt.Errorf("consumer/config: error loading config with topic names from env vars: %w", err)
	}

	return nil
}

func buildConfig(base Config) (*Config, error) {
	if err := base.loadFromEnvVars(); err != nil {
		return nil, err
	}

	return &base, nil
}
