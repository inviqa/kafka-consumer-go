package consumer

import (
	"context"

	"github.com/IBM/sarama"

	"github.com/revdaalex/kafka-consumer-go/config"
)

type Handler func(ctx context.Context, msg *sarama.ConsumerMessage) error
type HandlerMap map[config.TopicKey]Handler

func (hm HandlerMap) handlerForTopic(t config.TopicKey) (Handler, bool) {
	h, ok := hm[t]
	return h, ok
}
