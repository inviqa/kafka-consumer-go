package model

import (
	"encoding/json"

	"github.com/Shopify/sarama"
)

type Failure struct {
	Reason         string
	Topic          string
	NextTopic      string
	Message        []byte
	MessageKey     []byte
	MessageHeaders []byte
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
		MessageHeaders: saramaRecordHeadersToJson(sm.Headers),
		KafkaPartition: sm.Partition,
		KafkaOffset:    sm.Offset,
	}
}

func saramaRecordHeadersToJson(headers []*sarama.RecordHeader) []byte {
	headerMap := map[string]string{}

	for _, h := range headers {
		headerMap[string(h.Key)] = string(h.Value)
	}

	// we silence the error if the marshalling fails, as the data is likely not valid
	// anyway and there is nothing else we can do at this stage, in the future we may
	// look at bubbling up a special error to the caller, but it would need to be logged
	// and the message would still need to be sent for retry anyway
	headerJson, _ := json.Marshal(headerMap)

	return headerJson
}
