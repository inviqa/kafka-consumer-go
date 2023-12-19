package model

import (
	"github.com/Shopify/sarama"
)

type Failure struct {
	Reason         string
	Topic          string
	NextTopic      string
	Message        []byte
	MessageKey     []byte
	MessageHeaders []sarama.RecordHeader
	KafkaPartition int32
	KafkaOffset    int64
}

// FailureFromSaramaMessage will create a Failure value from the provided values.
// NOTE: In the future, if we drop support for kafka retry topics, the nextTopic
// parameter will be removed.
func FailureFromSaramaMessage(err error, nextTopic string, sm *sarama.ConsumerMessage) Failure {
	return Failure{
		Reason:         err.Error(),
		Topic:          sm.Topic,
		NextTopic:      nextTopic,
		Message:        sm.Value,
		MessageKey:     sm.Key,
		MessageHeaders: convertSaramaRecordHeaders(sm.Headers),
		KafkaPartition: sm.Partition,
		KafkaOffset:    sm.Offset,
	}
}

func convertSaramaRecordHeaders(headers []*sarama.RecordHeader) []sarama.RecordHeader {
	nonPointHeaders := make([]sarama.RecordHeader, len(headers))

	for i, h := range headers {
		nonPointHeaders[i].Key = h.Key
		nonPointHeaders[i].Value = h.Value
	}

	return nonPointHeaders
}
