package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	EnvVarHost           = "KAFKA_HOST"
	EnvVarGroup          = "KAFKA_GROUP"
	EnvVarSourceTopics   = "KAFKA_SOURCE_TOPICS"
	EnvVarRetryIntervals = "KAFKA_RETRY_INTERVALS"
	EnvVarTLSEnable      = "TLS_ENABLE"
	EnvVarTLSSkipVerify  = "TLS_SKIP_VERIFY_PEER"
)

type Config struct {
	Host               []string
	Group              string
	ConsumableTopics   []*KafkaTopic
	TopicMap           map[TopicKey]*KafkaTopic
	TLSEnable          bool
	TLSSkipVerifyPeer  bool
	topicNameGenerator topicNameGenerator
}

type KafkaTopic struct {
	Name  string
	Delay time.Duration
	Key   TopicKey
	Next  *KafkaTopic
}

type TopicKey string
type topicNameGenerator func(group, mainTopic, prefix string) string

func NewConfig() (*Config, error) {
	return buildConfig(Config{
		topicNameGenerator: defaultTopicNameGenerator,
	})
}

func (cfg *Config) AddTopics(topics []*KafkaTopic) {
	cfg.ConsumableTopics = append(cfg.ConsumableTopics, topics[:len(topics)-1]...)

	if cfg.TopicMap == nil {
		cfg.TopicMap = map[TopicKey]*KafkaTopic{}
	}
	for _, topic := range topics {
		cfg.TopicMap[TopicKey(topic.Name)] = topic
	}
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

func (cfg *Config) addTopicsFromSource(topics []string, retryIntervals []int) error {
	for _, topic := range topics {
		// main topic
		derivedTopics := []*KafkaTopic{
			{
				Name: topic,
				Key:  TopicKey(topic),
			},
		}

		// retry topics
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

			derivedTopics[i].Next = rt
			derivedTopics = append(derivedTopics, rt)
		}

		// deadLetter topic
		dt := &KafkaTopic{
			Name: cfg.topicNameGenerator(cfg.Group, topic, "deadLetter"),
			Key:  TopicKey(topic),
		}
		derivedTopics[len(derivedTopics)-1].Next = dt

		derivedTopics = append(derivedTopics, dt)

		cfg.AddTopics(derivedTopics)
	}

	return nil
}

func (cfg *Config) loadFromEnvVars() error {
	cfg.Host = strings.Split(os.Getenv(EnvVarHost), ",")
	cfg.Group = os.Getenv(EnvVarGroup)
	cfg.TLSEnable = envVarAsBool(EnvVarTLSEnable)
	cfg.TLSSkipVerifyPeer = envVarAsBool(EnvVarTLSSkipVerify)

	sourceTopics := strings.Split(os.Getenv(EnvVarSourceTopics), ",")
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
