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

func TestNewConfig(t *testing.T) {
	t.Run("it creates config correctly", func(t *testing.T) {
		setAllEnvVars()
		defer os.Clearenv()

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

		exp := &Config{
			Host:             []string{"broker1", "broker2"},
			Group:            "kafkaGroup",
			ConsumableTopics: []*KafkaTopic{expMainProduct, expRetry1Product, expRetry2Product},
			TopicMap: map[TopicKey]*KafkaTopic{
				"product":                       expMainProduct,
				"retry1.kafkaGroup.product":     expRetry1Product,
				"retry2.kafkaGroup.product":     expRetry2Product,
				"deadLetter.kafkaGroup.product": expDeadLetterProduct,
			},
			DBRetries: map[string][]*DBTopicRetry{
				"product": {
					{
						Interval: time.Duration(120000000000),
						Sequence: 1,
						Key:      "product",
					},
					{
						Interval: time.Duration(300000000000),
						Sequence: 2,
						Key:      "product",
					},
				},
			},
			TLSSkipVerifyPeer: true,
			DB: Database{
				Host:   "localhost",
				Port:   3306,
				Schema: "consumer",
				User:   "user",
				Pass:   "pass123",
			},
			UseDBForRetryQueue:  true,
			MaintenanceInterval: time.Second * 100,
		}

		c, err := NewConfig()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if diff := deep.Equal(c, exp); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("it creates config from only required env vars", func(t *testing.T) {
		setRequiredEnvVars()
		defer os.Clearenv()

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
			DBRetries:           DBRetries{"product": []*DBTopicRetry{}},
			MaintenanceInterval: time.Hour * 1,
		}

		c, err := NewConfig()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if diff := deep.Equal(c, exp); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("it returns error if KAFKA_HOST is omitted", func(t *testing.T) {
		setRequiredEnvVars()
		defer os.Clearenv()
		os.Setenv("KAFKA_HOST", "")

		_, err := NewConfig()
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})

	t.Run("it returns error if KAFKA_GROUP is omitted", func(t *testing.T) {
		setRequiredEnvVars()
		defer os.Clearenv()
		os.Setenv("KAFKA_GROUP", "")

		_, err := NewConfig()
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})

	t.Run("it returns error if KAFKA_SOURCE_TOPICS is omitted", func(t *testing.T) {
		setRequiredEnvVars()
		defer os.Clearenv()
		os.Setenv("KAFKA_SOURCE_TOPICS", "")

		_, err := NewConfig()
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})

	t.Run("it returns error if KAFKA_RETRY_INTERVALS is ommitted", func(t *testing.T) {
		setRequiredEnvVars()
		defer os.Clearenv()
		os.Setenv("KAFKA_RETRY_INTERVALS", "58493058409358439058349058903485309")

		_, err := NewConfig()
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func TestConfig_GetDBConnectionString(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "without TLS enabled",
			cfg: Config{
				DB: Database{
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
				DB: Database{
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
				DB: Database{
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
			if got := tt.cfg.DSN(); got != tt.want {
				t.Errorf("DSN(): %s, want %s", got, tt.want)
			}
		})
	}
}

func TestConfig_MainTopics(t *testing.T) {
	setRequiredEnvVars()
	defer os.Clearenv()

	t.Run("main topics returned", func(t *testing.T) {
		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		got := cfg.MainTopics()
		exp := []string{"product"}
		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})
}

func setAllEnvVars() {
	setRequiredEnvVars()
	os.Setenv("KAFKA_RETRY_INTERVALS", "120,300")
	os.Setenv("TLS_ENABLE", "false")
	os.Setenv("TLS_SKIP_VERIFY_PEER", "true")
	os.Setenv("DB_HOST", "localhost")
	os.Setenv("DB_PORT", "3306")
	os.Setenv("DB_USER", "user")
	os.Setenv("DB_PASS", "pass123")
	os.Setenv("DB_SCHEMA", "consumer")
	os.Setenv("USE_DB_RETRY_QUEUE", "true")
	os.Setenv("MAINTENANCE_INTERVAL_SECONDS", "100")
}

func setRequiredEnvVars() {
	os.Setenv("KAFKA_HOST", "broker1,broker2")
	os.Setenv("KAFKA_GROUP", "kafkaGroup")
	os.Setenv("KAFKA_SOURCE_TOPICS", "product")
}
