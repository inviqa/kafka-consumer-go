package config

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-test/deep"
)

func init() {
	deep.CompareUnexportedFields = true
}

func TestNewBuilder(t *testing.T) {
	exp := &Builder{
		topicNameGenerator: defaultTopicNameGenerator,
	}

	got := NewBuilder()
	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

func TestBuilder_Config(t *testing.T) {
	os.Args = nil
	os.Setenv("KAFKA_SOURCE_TOPICS", "product")
	os.Setenv("KAFKA_RETRY_INTERVALS", "120")
	os.Setenv("KAFKA_HOST", "broker1,broker2")
	os.Setenv("KAFKA_GROUP", "algolia")

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
		Group:            "algolia",
		ConsumableTopics: []*KafkaTopic{expMainProduct, expRetry1Product},
		TopicMap: map[TopicKey]*KafkaTopic{
			"product":               expMainProduct,
			"retry1.fixedTopic":     expRetry1Product,
			"deadLetter.fixedTopic": expDeadLetterProduct,
		},
	}

	c, err := NewBuilder().SetTopicNameDecorator(dummyGenerator).Config()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if diff := deep.Equal(c, exp); diff != nil {
		t.Error(diff)
	}

	os.Clearenv()
}
