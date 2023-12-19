package consumer

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/log"
)

type consumer struct {
	failureCh chan<- model.Failure
	cfg       *config.Config
	handlers  HandlerMap
	logger    log.Logger
}

func newConsumer(fch chan<- model.Failure, cfg *config.Config, hs HandlerMap, l log.Logger) sarama.ConsumerGroupHandler {
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

			messageTime := time.Now()
			var retryTime time.Time
			var needCheckRetryTime bool
			var err error

			if len(message.Headers) != 0 {
				for _, header := range message.Headers {
					if string(header.Key) == "NextTimeRetry" {
						retryTime, err = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", string(header.Value))
						if err != nil {
							return err
						}

						needCheckRetryTime = true
					}
				}
			}

			if needCheckRetryTime {
				if retryTime.Before(messageTime) {
					continue
				}
			}

			c.logger.Debugf("processing message from Kafka")

			k := c.cfg.FindTopicKey(message.Topic)
			h, ok := c.handlers.handlerForTopic(k)
			if !ok {
				return fmt.Errorf("consumer: handler not found for topic: %s", k)
			}

			if err = h(session.Context(), message); err != nil {
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
	nextTopic, nextErr := c.cfg.NextTopicInChain(message.Topic)
	if nextErr != nil {
		c.logger.Errorf("no next topic to send failure to (deadletter topic being consumed?)")
		return
	}

	netTimeRetry := time.Now().Add(nextTopic.Delay)

	if len(message.Headers) == 0 {
		message.Headers = make([]*sarama.RecordHeader, 0)
	}

	retryHeader := &sarama.RecordHeader{
		Key:   []byte("NextTimeRetry"),
		Value: []byte(netTimeRetry.String()),
	}

	message.Headers = append(message.Headers, retryHeader)

	c.failureCh <- model.FailureFromSaramaMessage(err, nextTopic.Name, message)
}

func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
