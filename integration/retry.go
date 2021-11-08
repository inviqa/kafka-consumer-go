package integration

import (
	"github.com/inviqa/kafka-consumer-go/data/retry/model"
)

type Retry struct {
	model.Retry
	Successful bool
	LastError  string
}
