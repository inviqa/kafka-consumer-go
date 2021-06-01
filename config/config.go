package config

import (
	"fmt"
	"log"
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
	Host              []string
	Group             string
	ConsumableTopics  []*KafkaTopic
	TopicMap          map[TopicKey]*KafkaTopic
	TLSEnable         bool
	TLSSkipVerifyPeer bool
}

type KafkaTopic struct {
	Name  string
	Delay time.Duration
	Key   TopicKey
	Next  *KafkaTopic
}

type TopicKey string

func NewConfig() *Config {
	a := &args{
		TLSEnable:         false,
		TLSSkipVerifyPeer: false,
	}
	arg.MustParse(a)
	c := &Config{
		Host:              a.KafkaHost,
		Group:             a.KafkaGroup,
		TLSEnable:         a.TLSEnable,
		TLSSkipVerifyPeer: a.TLSSkipVerifyPeer,
	}

	if a.KafkaSourceTopics != nil {
		c.addTopicsFromSource(a.KafkaGroup, a.KafkaSourceTopics, a.KafkaRetryIntervals)
	}

	return c
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

func (cfg *Config) addTopicsFromSource(group string, topics []string, retryIntervals []int) {
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
				log.Panicf("could not parse delay in seconds %d in KAFKA_RETRY_INTERVALS env var: %v", interval, topics)
			}
			rt := &KafkaTopic{
				Name:  fmt.Sprintf("retry%d.%s.%s", i+1, group, topic),
				Delay: d,
				Key:   TopicKey(topic),
			}

			if i == 0 {
				derivedTopics[0].Next = rt
			} else {
				derivedTopics[i].Next = rt
			}

			derivedTopics = append(derivedTopics, rt)
		}

		// deadLetter topic
		dt := &KafkaTopic{
			Name: fmt.Sprintf("deadLetter.%s.%s", group, topic),
			Key:  TopicKey(topic),
		}
		derivedTopics[len(derivedTopics)-1].Next = dt

		derivedTopics = append(derivedTopics, dt)

		cfg.AddTopics(derivedTopics)
	}
}
