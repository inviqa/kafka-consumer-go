package integration

import (
	"github.com/revdaalex/kafka-consumer-go/data/retry/model"
)

type Retry struct {
	model.Retry
	Successful bool
	LastError  string
}
