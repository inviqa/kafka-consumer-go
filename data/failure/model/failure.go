package model

import (
	"encoding/base64"
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
	headerMap := map[string][]byte{}

	for _, h := range headers {
		valueBytes, err := base64.StdEncoding.DecodeString(string(h.Value))
		// TODO: should we just store the original h.Value? this might give us the chance to inspect the data later if it's stored, e.g. in DB retry
		if err != nil {
			headerMap[string(h.Key)] = []byte(`{}`)
		} else {
			headerMap[string(h.Key)] = valueBytes
		}
	}

	// we silence the error if the marshalling fails, as the data is likely not valid
	// anyway and there is nothing else we can do at this stage, in the future we may
	// look at bubbling up a special error to the caller, but it would need to be logged
	// and the message would still need to be sent for retry anyway
	headerJson, _ := json.Marshal(headerMap)

	return headerJson
}
