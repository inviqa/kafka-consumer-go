package config

import (
	"fmt"
	"strconv"
	"time"

	"github.com/alexflint/go-arg"
)

type args struct {
	KafkaHost           []string `arg:"--kafka-host,env:KAFKA_HOST,required"`
	KafkaGroup          string   `arg:"--kafka-group,env:KAFKA_GROUP"`
	KafkaSourceTopics   []string `arg:"--kafka-source-topics,env:KAFKA_SOURCE_TOPICS"`
	KafkaRetryIntervals []int    `arg:"--kafka-retry-intervals,env:KAFKA_RETRY_INTERVALS"`
	TLSEnable           bool     `arg:"--kafka-tls,env:TLS_ENABLE"`
	TLSSkipVerifyPeer   bool     `arg:"--kafka-tls-skip-verify-peer,env:TLS_SKIP_VERIFY_PEER"`
}

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

func (cfg *Config) addTopicsFromSource(group string, topics []string, retryIntervals []int) error {
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
				Name:  cfg.topicNameGenerator(group, topic, fmt.Sprintf("retry%d", i+1)),
				Delay: d,
				Key:   TopicKey(topic),
			}

			derivedTopics[i].Next = rt
			derivedTopics = append(derivedTopics, rt)
		}

		// deadLetter topic
		dt := &KafkaTopic{
			Name: cfg.topicNameGenerator(group, topic, "deadLetter"),
			Key:  TopicKey(topic),
		}
		derivedTopics[len(derivedTopics)-1].Next = dt

		derivedTopics = append(derivedTopics, dt)

		cfg.AddTopics(derivedTopics)
	}

	return nil
}

func buildConfig(base Config) (*Config, error) {
	a := &args{
		TLSEnable:         false,
		TLSSkipVerifyPeer: false,
	}

	if err := arg.Parse(a); err != nil {
		return nil, fmt.Errorf("consumer/config: error parsing configuration from env vars: %w", err)
	}

	base.Host = a.KafkaHost
	base.Group = a.KafkaGroup
	base.TLSEnable = a.TLSEnable
	base.TLSSkipVerifyPeer = a.TLSSkipVerifyPeer

	if a.KafkaSourceTopics != nil {
		err := base.addTopicsFromSource(a.KafkaGroup, a.KafkaSourceTopics, a.KafkaRetryIntervals)
		if err != nil {
			return nil, fmt.Errorf("consumer/config: error decorating config with topic names: %w", err)
		}
	}

	return &base, nil
}

func defaultTopicNameGenerator(group, topic, prefix string) string {
	return fmt.Sprintf("%s.%s.%s", prefix, group, topic)
}
