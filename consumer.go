package consumer

import (
	"fmt"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/log"
)

type consumer struct {
	failureCh chan<- data.Failure
	cfg       *config.Config
	handlers HandlerMap
	logger   log.Logger
}

type Handler func(msg *sarama.ConsumerMessage) error
type HandlerMap map[config.TopicKey]Handler

func NewConsumer(fch chan<- data.Failure, cfg *config.Config, hs HandlerMap, l log.Logger) sarama.ConsumerGroupHandler {
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
				return fmt.Errorf("consumer: handler not found for topic: %s", k)
			}

			if err := h(message); err != nil {
				c.sendToFailureChannel(message, err)
			}

			c.markMessageProcessed(session, message)
		case <-session.Context().Done():
			c.logger.Debug("consumer: session context finished, returning")
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

	c.failureCh <- data.Failure{
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
