package test

import (
	"os"

	"github.com/inviqa/kafka-consumer-go/config"
)

// NewConfig will set env vars from the provided raw values, and then return a new
// config.Config created from those values. It is intended for test use only.
// NOTE: This method does not clean up any env vars that it sets.
// #nosec G104
func NewConfig(host, group, sourceTopics, retryIntervals string) (*config.Config, error) {
	os.Setenv(config.EnvVarHost, host)
	os.Setenv(config.EnvVarGroup, group)
	os.Setenv(config.EnvVarSourceTopics, sourceTopics)
	os.Setenv(config.EnvVarRetryIntervals, retryIntervals)

	return config.NewConfig()
}
