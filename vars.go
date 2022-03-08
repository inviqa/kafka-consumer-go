package consumer

import "time"

var (
	maxConnectionAttempts      = 20
	connectionInterval         = time.Second * 1
	dbRetryPollInterval        = time.Second * 5
	defaultMaintenanceInterval = time.Hour * 1
	defaultKafkaConnector      = connectToKafka
)
