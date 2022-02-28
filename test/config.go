package test

import (
	"github.com/inviqa/kafka-consumer-go/config"
)

// NewConfig will set env vars from the provided raw values, and then return a new
// config.Config created from those values. It is intended for test use only.
// NOTE: This method does not clean up any env vars that it sets.
// #nosec G104
func NewConfig(host, group string, sourceTopics []string, retryIntervals []int) (*config.Config, error) {
	return config.NewBuilder().
		SetKafkaGroup(group).
		SetKafkaHost([]string{host}).
		SetSourceTopics(sourceTopics).
		SetRetryIntervals(retryIntervals).
		Config()
}
