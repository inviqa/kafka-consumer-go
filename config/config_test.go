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

func TestNewConfig(t *testing.T) {
	os.Args = nil
	os.Setenv("KAFKA_TOPICS", "retryTopic2mins:120,retryTopic5mins:300,deadLetterTopic")
	os.Setenv("KAFKA_DEAD_LETTER_TOPIC", "deadLetterTopic")
	os.Setenv("KAFKA_HOST", "broker1,broker2")
	os.Setenv("KAFKA_GROUP", "kafkaGroup")
	os.Setenv("TLS_ENABLE", "false")
	os.Setenv("TLS_VERIFY_PEER", "false")

	exp3 := &KafkaTopic{
		Name: "deadLetterTopic",
		Key:  "default",
	}
	exp2 := &KafkaTopic{
		Name:  "retryTopic5mins",
		Delay: time.Duration(300000000000),
		Key:   "default",
		Next:  exp3,
	}
	exp1 := &KafkaTopic{
		Name:  "retryTopic2mins",
		Delay: time.Duration(120000000000),
		Key:   "default",
		Next:  exp2,
	}

	exp := &Config{
		Host:             []string{"broker1", "broker2"},
		Group:            "kafkaGroup",
		ConsumableTopics: []*KafkaTopic{exp1, exp2},
		TopicMap: map[string]*KafkaTopic{
			"retryTopic2mins": exp1,
			"retryTopic5mins": exp2,
			"deadLetterTopic": exp3,
		},
	}

	c := NewConfig()
	if diff := deep.Equal(c, exp); diff != nil {
		t.Error(diff)
	}

	os.Clearenv()
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
		TopicMap: map[string]*KafkaTopic{
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
		TopicMap: map[string]*KafkaTopic{
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
		TopicMap: map[string]*KafkaTopic{
			"firstRetry":  exp1,
			"secondRetry": exp2,
		},
	}

	_, err := cfg.NextTopicNameInChain("missing")
	if err == nil {
		t.Errorf("expected error as topics not in configured topics")
	}
}

func TestParseTopics(t *testing.T) {
	exp3 := &KafkaTopic{
		Name: "deadLetter",
		Key:  "default",
	}
	exp2 := &KafkaTopic{
		Name:  "second",
		Delay: 60000000000,
		Key:   "default",
		Next:  exp3,
	}
	exp1 := &KafkaTopic{
		Name:  "first",
		Delay: 0,
		Key:   "default",
		Next:  exp2,
	}

	type args struct {
		key    string
		topics []string
	}
	tests := []struct {
		name string
		args args
		want []*KafkaTopic
	}{
		{
			name: "parse topics from strings",
			args: args{key: "default", topics: []string{"first:0", "second:60", "deadLetter"}},
			want: []*KafkaTopic{exp1, exp2, exp3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTopics(tt.args.key, tt.args.topics)
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestConfig_AddTopics(t *testing.T) {
	type fields struct {
		Host             []string
		Group            string
		ConsumableTopics []*KafkaTopic
		TopicMap         map[string]*KafkaTopic
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
		expTopicMap         map[string]*KafkaTopic
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
			expTopicMap: map[string]*KafkaTopic{
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
			expTopicMap: map[string]*KafkaTopic{
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
				TopicMap: map[string]*KafkaTopic{
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
			expTopicMap: map[string]*KafkaTopic{
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
			cfg.AddTopics(tt.args.topicKey, tt.args.topics)

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
		TopicMap         map[string]*KafkaTopic
	}
	type args struct {
		topicName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "Finds topic",
			fields: fields{
				TopicMap: map[string]*KafkaTopic{
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
				TopicMap: map[string]*KafkaTopic{
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
