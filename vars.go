package consumer

import "time"

var (
	maxConnectionAttempts      = 10
	connectionInterval         = time.Millisecond * 500
	dbRetryPollInterval        = time.Second * 5
	defaultMaintenanceInterval = time.Hour * 1
	defaultKafkaConnector      = connectToKafka
)
