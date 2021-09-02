package consumer

import "time"

var (
	maxConnectionAttempts = 10
	connectionInterval    = time.Millisecond * 500
	dbRetryPollInterval   = time.Second * 5
)
