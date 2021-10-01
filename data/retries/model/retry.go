package model

import (
	"encoding/json"

	"github.com/Shopify/sarama"
)

type Retry struct {
	ID             int64
	Topic          string
	PayloadJSON    []byte
	PayloadHeaders []byte
	PayloadKey     []byte
	KafkaOffset    int64
	KafkaPartition int32
	Attempts       uint8
	Deadlettered   bool
	Errored        bool
}

type recordHeaders map[string]string

func (r Retry) ToSaramaConsumerMessage() *sarama.ConsumerMessage {
	msg := &sarama.ConsumerMessage{
		Key:       r.PayloadKey,
		Value:     r.PayloadJSON,
		Topic:     r.Topic,
		Partition: r.KafkaPartition,
		Offset:    r.KafkaOffset,
	}

	var rh recordHeaders
	if err := json.Unmarshal(r.PayloadHeaders, &rh); err == nil {
		for k, v := range rh {
			msg.Headers = append(msg.Headers, &sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}

	return msg
}
