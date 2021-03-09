package config

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/alexflint/go-arg"
)

type args struct {
	KafkaHost         []string `arg:"--kafka-host,env:KAFKA_HOST,required"`
	KafkaGroup        string   `arg:"--kafka-group,env:KAFKA_GROUP"`
	KafkaTopics       []string `arg:"--kafka-topics,env:KAFKA_TOPICS"`
	TLSEnable         bool     `arg:"--kafka-tls,env:TLS_ENABLE"`
	TLSSkipVerifyPeer bool     `arg:"--kafka-tls-skip-verify-peer,env:TLS_SKIP_VERIFY_PEER"`
}

type Config struct {
	Host              []string
	Group             string
	ConsumableTopics  []*KafkaTopic
	TopicMap          map[string]*KafkaTopic
	TLSEnable         bool
	TLSSkipVerifyPeer bool
}

type KafkaTopic struct {
	Name  string
	Delay time.Duration
	Key   string
	Next  *KafkaTopic
}

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

	if a.KafkaTopics != nil {
		c.AddTopicsFromStrings("default", a.KafkaTopics)
	}

	return c
}

func parseTopics(topicKey string, topics []string) []*KafkaTopic {
	parsedTopics := make([]*KafkaTopic, len(topics))
	for i, t := range topics {
		name := t
		delay := time.Duration(0)
		if !isDeadLetterTopic(i, topics) {
			p := strings.Split(t, ":")
			name = p[0]
			var err error
			delay, err = time.ParseDuration(p[1] + "s")
			if err != nil {
				log.Panicf("could not parse delay in seconds %s in KAFKA_TOPICS env var: %v", p[1], topics)
			}
		}

		topic := &KafkaTopic{
			Name:  name,
			Key:   topicKey,
			Delay: delay,
		}
		if i != 0 {
			parsedTopics[i-1].Next = topic
		}
		parsedTopics[i] = topic
	}

	return parsedTopics
}

func isDeadLetterTopic(i int, topics []string) bool {
	return i == (len(topics) - 1)
}

func (cfg *Config) AddTopicsFromStrings(topicKey string, topics []string) {
	cfg.AddTopics(topicKey, parseTopics(topicKey, topics))
}

func (cfg *Config) AddTopics(topicKey string, topics []*KafkaTopic) {
	cfg.ConsumableTopics = append(cfg.ConsumableTopics, topics[:len(topics)-1]...)

	if cfg.TopicMap == nil {
		cfg.TopicMap = map[string]*KafkaTopic{}
	}
	for _, topic := range topics {
		cfg.TopicMap[topic.Name] = topic
	}
}

func (cfg *Config) NextTopicNameInChain(currentTopic string) (string, error) {
	topic, ok := cfg.TopicMap[currentTopic]
	if !ok {
		return "", fmt.Errorf("topic not found")
	}

	next := topic.Next

	if next == nil {
		return "", fmt.Errorf("there is no next topic in the chain")
	}

	return next.Name, nil
}

func (cfg *Config) FindTopicKey(topicName string) string {
	topic, ok := cfg.TopicMap[topicName]
	if !ok {
		return "default"
	}

	return topic.Key
}
