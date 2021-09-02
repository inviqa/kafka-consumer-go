package retries

import (
	"encoding/json"
	"time"
)

type Retry struct {
	ID             int64
	Topic          string
	PayloadJSON    json.RawMessage
	PayloadHeaders json.RawMessage
	PayloadKey     []byte
	// TODO: do we need these??
	KafkaOffset    int32
	KafkaPartition int64
	Attempts       uint8
	DeadLettered   bool
	Successful     bool
	LastError      error
	CreatedAt      time.Time
	Updated        time.Time
}
