package config

import (
	"testing"

	"github.com/go-test/deep"
)

func init() {
	deep.CompareUnexportedFields = true
}

func TestConfig_NextTopicNameInChain(t *testing.T) {
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

	t.Run("it gets next topic name in the chain", func(t *testing.T) {
		next, _ := cfg.NextTopicNameInChain("firstRetry")
		if next != "secondRetry" {
			t.Errorf("expected 'secondRetry' topic name, but got '%s'", next)
		}
	})

	t.Run("it errors if there is no next topic", func(t *testing.T) {
		_, err := cfg.NextTopicNameInChain("secondRetry")
		if err == nil {
			t.Errorf("expected error as no more topics but did not get one")
		}
	})

	t.Run("it errors if the topic name is not found", func(t *testing.T) {
		_, err := cfg.NextTopicNameInChain("missing")
		if err == nil {
			t.Errorf("expected error as topics not in configured topics")
		}
	})
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
			cfg.addTopics(tt.args.topics)

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

func TestConfig_dsn(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "without TLS enabled",
			cfg: Config{
				db: Database{
                    Driver: "postgres",
					Host:   "postgres-db",
					Port:   5002,
					Schema: "data",
					User:   "root",
					Pass:   "pass123",
				},
			},
			want: "postgres://root:pass123@postgres-db:5002/data?sslmode=disable",
		},
		{
			name: "with TLS enabled",
			cfg: Config{
				db: Database{
                    Driver: "postgres",
					Host:   "postgres-db",
					Port:   5002,
					Schema: "data",
					User:   "root",
					Pass:   "pass123",
				},
				TLSEnable: true,
			},
			want: "postgres://root:pass123@postgres-db:5002/data?sslmode=verify-full",
		},
		{
			name: "with password that should be encoded",
			cfg: Config{
				db: Database{
                    Driver: "postgres",
					Host:   "postgres-db",
					Port:   5002,
					Schema: "data",
					User:   "root",
					Pass:   "pass%123",
				},
				TLSEnable: true,
			},
			want: "postgres://root:pass%25123@postgres-db:5002/data?sslmode=verify-full",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.dsn(); got != tt.want {
				t.Errorf("dsn(): %s, want %s", got, tt.want)
			}
		})
	}
}

func TestConfig_MainTopics(t *testing.T) {
	t.Run("main topics returned", func(t *testing.T) {
		cfg := Config{
			Host:  []string{"broker1", "broker2"},
			Group: "kafkaGroup",
			ConsumableTopics: []*KafkaTopic{
				{
					Name:  "product",
					Delay: 0,
					Key:   "product",
				},
			},
		}
		got := cfg.MainTopics()
		exp := []string{"product"}
		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})
}
