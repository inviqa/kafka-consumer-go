package config

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-test/deep"
)

func init() {
	deep.CompareUnexportedFields = true
}

func TestNewBuilder(t *testing.T) {
	exp := &Builder{
		dBPort:              5432,
		maintenanceInterval: time.Hour * 1,
		topicNameGenerator:  defaultTopicNameGenerator,
	}

	got := NewBuilder()
	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

func TestBuilder_Config(t *testing.T) {
	t.Run("it creates expected configuration", func(t *testing.T) {
		dummyGenerator := func(group, mainTopic, prefix string) string {
			return fmt.Sprintf("%s.fixedTopic", prefix)
		}

		expDeadLetterProduct := &KafkaTopic{
			Name: "deadLetter.fixedTopic",
			Key:  "product",
		}
		expRetry1Product := &KafkaTopic{
			Name:  "retry1.fixedTopic",
			Delay: time.Duration(120000000000),
			Key:   "product",
			Next:  expDeadLetterProduct,
		}
		expMainProduct := &KafkaTopic{
			Name: "product",
			Key:  "product",
			Next: expRetry1Product,
		}
		exp := &Config{
			Host:             []string{"broker1", "broker2"},
			Group:            "group",
			ConsumableTopics: []*KafkaTopic{expMainProduct, expRetry1Product},
			TopicMap: map[TopicKey]*KafkaTopic{
				"product":               expMainProduct,
				"retry1.fixedTopic":     expRetry1Product,
				"deadLetter.fixedTopic": expDeadLetterProduct,
			},
			DBRetries: map[string][]*DBTopicRetry{
				"product": {
					{
						Interval: time.Duration(120000000000),
						Sequence: 1,
						Key:      "product",
					},
				},
			},
			DB: Database{
				Host:   "postgres",
				Port:   15432,
				Schema: "schema",
				User:   "user",
				Pass:   "pass",
			},
			MaintenanceInterval: time.Hour * 2,
			TLSEnable:           true,
			TLSSkipVerifyPeer:   true,
			UseDBForRetryQueue:  true,
		}

		c, err := NewBuilder().
			SetTopicNameGenerator(dummyGenerator).
			SetKafkaHost([]string{"broker1", "broker2"}).
			SetKafkaGroup("group").
			SetSourceTopics([]string{"product"}).
			SetRetryIntervals([]int{120}).
			SetDBHost("postgres").
			SetDBPass("pass").
			SetDBUser("user").
			SetDBSchema("schema").
			SetDBPort(15432).
			UseDbForRetries(true).
			EnableTLS(true).
			SkipTLSVerifyPeer(true).
			SetMaintenanceInterval(time.Hour * 2).
			Config()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if diff := deep.Equal(c, exp); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("it uses sensible defaults for optional fields", func(t *testing.T) {
		expDeadLetterProduct := &KafkaTopic{
			Name: "deadLetter.group.product",
			Key:  "product",
		}
		expMainProduct := &KafkaTopic{
			Name: "product",
			Key:  "product",
			Next: expDeadLetterProduct,
		}
		exp := &Config{
			Host:             []string{"broker1", "broker2"},
			Group:            "group",
			ConsumableTopics: []*KafkaTopic{expMainProduct},
			TopicMap: map[TopicKey]*KafkaTopic{
				"product":                  expMainProduct,
				"deadLetter.group.product": expDeadLetterProduct,
			},
			DBRetries: map[string][]*DBTopicRetry{
				"product": {},
			},
			DB: Database{
				Host:   "postgres",
				Port:   5432,
				Schema: "schema",
				User:   "user",
				Pass:   "pass",
			},
			MaintenanceInterval: time.Hour * 1,
		}

		c, err := NewBuilder().
			SetKafkaHost([]string{"broker1", "broker2"}).
			SetKafkaGroup("group").
			SetSourceTopics([]string{"product"}).
			SetDBHost("postgres").
			SetDBPass("pass").
			SetDBUser("user").
			SetDBSchema("schema").
			Config()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if diff := deep.Equal(exp, c); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("it returns an error if kafka host is not set", func(t *testing.T) {
		_, err := NewBuilder().
			SetKafkaGroup("group").
			SetSourceTopics([]string{"product"}).
			SetDBHost("postgres").
			SetDBPass("pass").
			SetDBUser("user").
			SetDBSchema("schema").
			Config()

		if err == nil {
			t.Error("expected an error but got nil")
		}
	})

	t.Run("it returns an error if kafka host is not set", func(t *testing.T) {
		_, err := NewBuilder().
			SetKafkaHost([]string{"broker1"}).
			SetSourceTopics([]string{"product"}).
			SetDBHost("postgres").
			SetDBPass("pass").
			SetDBUser("user").
			SetDBSchema("schema").
			Config()

		if err == nil {
			t.Error("expected an error but got nil")
		}
	})

	t.Run("it returns an error if no source topics are set", func(t *testing.T) {
		_, err := NewBuilder().
			SetKafkaHost([]string{"broker1"}).
			SetKafkaGroup("group").
			SetDBHost("postgres").
			SetDBPass("pass").
			SetDBUser("user").
			SetDBSchema("schema").
			Config()

		if err == nil {
			t.Error("expected an error but got nil")
		}
	})
}
