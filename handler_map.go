package consumer

import (
	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
)

type Handler func(msg *sarama.ConsumerMessage) error
type HandlerMap map[config.TopicKey]Handler

func (hm HandlerMap) handlerForTopic(t config.TopicKey) (Handler, bool) {
	h, ok := hm[t]
	return h, ok
}
