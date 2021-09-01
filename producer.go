package consumer

import (
	"context"
	"sync"
)

type failureProducer interface {
	listenForFailures(ctx context.Context, wg *sync.WaitGroup)
}
