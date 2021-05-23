package consumer

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/inviqa/kafka-consumer-go/config"
)

type consumer struct {
	failureCh chan<- Failure
	cfg       *config.Config
	handlers  HandlerMap
	logger    Logger
}

type Handler func(msg *sarama.ConsumerMessage) error
type HandlerMap map[string]Handler

func NewConsumer(fch chan<- Failure, cfg *config.Config, hs HandlerMap, l Logger) sarama.ConsumerGroupHandler {
	return &consumer{
		failureCh: fch,
		cfg:       cfg,
		handlers:  hs,
		logger:    l,
	}
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			c.logger.Debugf("processing message from Kafka")

			k := c.cfg.FindTopicKey(message.Topic)
			h, ok := c.handlers[k]
			if !ok {
				log.Panicf("Handler not found for topic: %s", k)
			}

			err := h(message)
			if err != nil {
				c.sendToFailureChannel(message, err)
			}

			c.markMessageProcessed(session, message)
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *consumer) markMessageProcessed(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	c.logger.Debugf("marking messages as processed")
	session.MarkMessage(msg, "")
}

func (c *consumer) sendToFailureChannel(message *sarama.ConsumerMessage, err error) {
	topic, nextErr := c.cfg.NextTopicNameInChain(message.Topic)
	if nextErr != nil {
		c.logger.Errorf("no next topic to send failure to (deadletter topic being consumed?)")
		return
	}

	c.failureCh <- Failure{
		Reason:        err.Error(),
		Message:       message.Value,
		TopicToSendTo: topic,
	}
}

func (c *consumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}
