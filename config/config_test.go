package config

import (
	"os"
	"testing"
	"time"

	"github.com/go-test/deep"
)

func init() {
	deep.CompareUnexportedFields = true
}

func TestNextTopicNameInChain(t *testing.T) {
	exp2 := &KafkaTopic{
		Name:  "secondRetry",
		Delay: 2,
		Key:   "topicKey",
	}
	exp1 := &KafkaTopic{
		Name:  "firstRetry",
		Delay: 1,
		Next:  exp2,
		Key:   "topicKey",
	}

	cfg := &Config{
		TopicMap: map[TopicKey]*KafkaTopic{
			"firstRetry":  exp1,
			"secondRetry": exp2,
		},
	}

	next, _ := cfg.NextTopicNameInChain("firstRetry")
	if next != "secondRetry" {
		t.Errorf("expected 'secondRetry' topic name, but got '%s'", next)
	}
}

func TestNextTopicNameInChain_ErrorIfLast(t *testing.T) {
	exp2 := &KafkaTopic{
		Name:  "secondRetry",
		Delay: 2,
		Key:   "topicKey",
	}
	exp1 := &KafkaTopic{
		Name:  "firstRetry",
		Delay: 1,
		Next:  exp2,
		Key:   "topicKey",
	}

	cfg := &Config{
		TopicMap: map[TopicKey]*KafkaTopic{
			"firstRetry":  exp1,
			"secondRetry": exp2,
		},
	}

	_, err := cfg.NextTopicNameInChain("secondRetry")
	if err == nil {
		t.Errorf("expected error as no more topics but did not get one")
	}
}

func TestNextTopicNameInChain_ErrorIfNotFound(t *testing.T) {
	exp2 := &KafkaTopic{
		Name:  "secondRetry",
		Delay: 2,
		Key:   "topicKey",
	}
	exp1 := &KafkaTopic{
		Name:  "firstRetry",
		Delay: 1,
		Next:  exp2,
		Key:   "topicKey",
	}

	cfg := &Config{
		TopicMap: map[TopicKey]*KafkaTopic{
			"firstRetry":  exp1,
			"secondRetry": exp2,
		},
	}

	_, err := cfg.NextTopicNameInChain("missing")
	if err == nil {
		t.Errorf("expected error as topics not in configured topics")
	}
}

func TestConfig_AddTopics(t *testing.T) {
	type fields struct {
		Host             []string
		Group            string
		ConsumableTopics []*KafkaTopic
		TopicMap         map[TopicKey]*KafkaTopic
	}
	type args struct {
		topicKey string
		topics   []*KafkaTopic
	}
	tests := []struct {
		name                string
		fields              fields
		args                args
		expConsumableTopics []*KafkaTopic
		expTopicMap         map[TopicKey]*KafkaTopic
	}{
		{
			name: "Add to nil",
			fields: fields{
				ConsumableTopics: nil,
			},
			args: args{
				topicKey: "default",
				topics: []*KafkaTopic{
					{
						Name:  "first",
						Delay: 0,
					},
					{
						Name:  "second",
						Delay: 1,
					},
					{
						Name: "deadLetter",
					},
				}},
			expConsumableTopics: []*KafkaTopic{
				{
					Name:  "first",
					Delay: 0,
				},
				{
					Name:  "second",
					Delay: 1,
				},
			},
			expTopicMap: map[TopicKey]*KafkaTopic{
				"first": {
					Name:  "first",
					Delay: 0,
				},
				"second": {
					Name:  "second",
					Delay: 1,
				},
				"deadLetter": {
					Name: "deadLetter",
				},
			},
		},
		{
			name: "Add to empty slice",
			fields: fields{
				ConsumableTopics: []*KafkaTopic{},
			},
			args: args{
				topicKey: "default",
				topics: []*KafkaTopic{
					{
						Name:  "first",
						Delay: 0,
					},
					{
						Name:  "second",
						Delay: 1,
					},
					{
						Name: "deadLetter",
					},
				}},
			expConsumableTopics: []*KafkaTopic{
				{
					Name:  "first",
					Delay: 0,
				},
				{
					Name:  "second",
					Delay: 1,
				},
			},
			expTopicMap: map[TopicKey]*KafkaTopic{
				"first": {
					Name:  "first",
					Delay: 0,
				},
				"second": {
					Name:  "second",
					Delay: 1,
				},
				"deadLetter": {
					Name: "deadLetter",
				},
			},
		},
		{
			name: "Add to existing topics",
			fields: fields{
				ConsumableTopics: []*KafkaTopic{
					{
						Name:  "alreadyThere",
						Delay: 0,
					},
				},
				TopicMap: map[TopicKey]*KafkaTopic{
					"alreadyThere": {
						Name:  "alreadyThere",
						Delay: 0,
					},
				},
			},
			args: args{
				topicKey: "price",
				topics: []*KafkaTopic{
					{
						Name:  "first",
						Delay: 0,
					},
					{
						Name:  "second",
						Delay: 1,
					},
					{
						Name: "deadLetter",
					},
				}},
			expConsumableTopics: []*KafkaTopic{
				{
					Name:  "alreadyThere",
					Delay: 0,
				},
				{
					Name:  "first",
					Delay: 0,
				},
				{
					Name:  "second",
					Delay: 1,
				},
			},
			expTopicMap: map[TopicKey]*KafkaTopic{
				"alreadyThere": {
					Name:  "alreadyThere",
					Delay: 0,
				},
				"first": {
					Name:  "first",
					Delay: 0,
				},
				"second": {
					Name:  "second",
					Delay: 1,
				},
				"deadLetter": {
					Name: "deadLetter",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Host:             tt.fields.Host,
				Group:            tt.fields.Group,
				ConsumableTopics: tt.fields.ConsumableTopics,
				TopicMap:         tt.fields.TopicMap,
			}
			cfg.AddTopics(tt.args.topics)

			if diff := deep.Equal(cfg.ConsumableTopics, tt.expConsumableTopics); diff != nil {
				t.Error(diff)
			}

			if diff := deep.Equal(cfg.TopicMap, tt.expTopicMap); diff != nil {
				t.Error(diff)
			}

		})
	}
}

func TestConfig_FindTopicKey(t *testing.T) {
	type fields struct {
		Host             []string
		Group            string
		ClientId         string
		ConsumableTopics []*KafkaTopic
		TopicMap         map[TopicKey]*KafkaTopic
	}
	type args struct {
		topicName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   TopicKey
	}{
		{
			name: "Finds topic",
			fields: fields{
				TopicMap: map[TopicKey]*KafkaTopic{
					"notThisOne": {
						Name:  "notThisOne",
						Delay: 0,
						Key:   "default",
					},
					"norThisOne": {
						Name:  "norThisOne",
						Delay: 0,
						Key:   "prices",
					},
					"thisOne": {
						Name:  "thisOne",
						Delay: 0,
						Key:   "prices",
					},
				},
			},
			args: args{topicName: "thisOne"},
			want: "prices",
		},
		{
			name: "Default if topic not found",
			fields: fields{
				TopicMap: map[TopicKey]*KafkaTopic{
					"notThisOne": {
						Name:  "notThisOne",
						Delay: 0,
						Key:   "default",
					},
					"norThisOne": {
						Name:  "norThisOne",
						Delay: 0,
						Key:   "prices",
					},
				},
			},
			args: args{topicName: "thisOne"},
			want: "default",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Host:             tt.fields.Host,
				Group:            tt.fields.Group,
				ConsumableTopics: tt.fields.ConsumableTopics,
				TopicMap:         tt.fields.TopicMap,
			}
			if got := cfg.FindTopicKey(tt.args.topicName); got != tt.want {
				t.Errorf("FindTopicKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewConfig(t *testing.T) {
	os.Setenv("KAFKA_SOURCE_TOPICS", "product,payment")
	os.Setenv("KAFKA_RETRY_INTERVALS", "120,300")
	os.Setenv("KAFKA_HOST", "broker1,broker2")
	os.Setenv("KAFKA_GROUP", "kafkaGroup")
	os.Setenv("TLS_ENABLE", "false")
	os.Setenv("TLS_VERIFY_PEER", "false")

	expDeadLetterProduct := &KafkaTopic{
		Name: "deadLetter.kafkaGroup.product",
		Key:  "product",
	}
	expRetry2Product := &KafkaTopic{
		Name:  "retry2.kafkaGroup.product",
		Delay: time.Duration(300000000000),
		Key:   "product",
		Next:  expDeadLetterProduct,
	}
	expRetry1Product := &KafkaTopic{
		Name:  "retry1.kafkaGroup.product",
		Delay: time.Duration(120000000000),
		Key:   "product",
		Next:  expRetry2Product,
	}
	expMainProduct := &KafkaTopic{
		Name: "product",
		Key:  "product",
		Next: expRetry1Product,
	}
	expDeadLetterPayment := &KafkaTopic{
		Name: "deadLetter.kafkaGroup.payment",
		Key:  "payment",
	}
	expRetry2Payment := &KafkaTopic{
		Name:  "retry2.kafkaGroup.payment",
		Delay: time.Duration(300000000000),
		Key:   "payment",
		Next:  expDeadLetterPayment,
	}
	expRetry1Payment := &KafkaTopic{
		Name:  "retry1.kafkaGroup.payment",
		Delay: time.Duration(120000000000),
		Key:   "payment",
		Next:  expRetry2Payment,
	}
	expMainPayment := &KafkaTopic{
		Name: "payment",
		Key:  "payment",
		Next: expRetry1Payment,
	}

	exp := &Config{
		Host:             []string{"broker1", "broker2"},
		Group:            "kafkaGroup",
		ConsumableTopics: []*KafkaTopic{expMainProduct, expRetry1Product, expRetry2Product, expMainPayment, expRetry1Payment, expRetry2Payment},
		TopicMap: map[TopicKey]*KafkaTopic{
			"product":                       expMainProduct,
			"retry1.kafkaGroup.product":     expRetry1Product,
			"retry2.kafkaGroup.product":     expRetry2Product,
			"deadLetter.kafkaGroup.product": expDeadLetterProduct,
			"payment":                       expMainPayment,
			"retry1.kafkaGroup.payment":     expRetry1Payment,
			"retry2.kafkaGroup.payment":     expRetry2Payment,
			"deadLetter.kafkaGroup.payment": expDeadLetterPayment,
		},
	}

	c, err := NewConfig()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if diff := deep.Equal(c, exp); diff != nil {
		t.Error(diff)
	}

	os.Clearenv()
}

func TestNewConfig_WithEmptyRetryInternals(t *testing.T) {
	os.Setenv("KAFKA_SOURCE_TOPICS", "product")
	os.Setenv("KAFKA_HOST", "broker1,broker2")
	os.Setenv("KAFKA_GROUP", "kafkaGroup")
	os.Setenv("TLS_ENABLE", "false")
	os.Setenv("TLS_VERIFY_PEER", "false")

	expDeadLetterProduct := &KafkaTopic{
		Name: "deadLetter.kafkaGroup.product",
		Key:  "product",
	}
	expMainProduct := &KafkaTopic{
		Name: "product",
		Key:  "product",
		Next: expDeadLetterProduct,
	}

	exp := &Config{
		Host:             []string{"broker1", "broker2"},
		Group:            "kafkaGroup",
		ConsumableTopics: []*KafkaTopic{expMainProduct},
		TopicMap: map[TopicKey]*KafkaTopic{
			"product":                       expMainProduct,
			"deadLetter.kafkaGroup.product": expDeadLetterProduct,
		},
	}

	c, err := NewConfig()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if diff := deep.Equal(c, exp); diff != nil {
		t.Error(diff)
	}

	os.Clearenv()
}

func TestNewConfig_WithError(t *testing.T) {
	defer func() {
		os.Clearenv()
	}()

	t.Run("missing KAFKA_HOST", func(t *testing.T) {
		_, err := NewConfig()
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})

	t.Run("invalid KAFKA_RETRY_INTERVALS", func(t *testing.T) {
		os.Setenv("KAFKA_HOST", "kafka:9092")
		os.Setenv("KAFKA_RETRY_INTERVALS", "58493058409358439058349058903485309")

		_, err := NewConfig()
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})
}
