package consumer

import (
	"context"
	"sync"

	"github.com/inviqa/kafka-consumer-go/data/failure/model"
)

type mockFailureProducer struct {
	sync.Mutex
	fch               chan model.Failure
	lastFailure       *model.Failure
	failureRecvdCount int
}

func newMockFailureProducer(fch chan model.Failure) *mockFailureProducer {
	return &mockFailureProducer{
		fch: fch,
	}
}

func (m *mockFailureProducer) listenForFailures(ctx context.Context, wg *sync.WaitGroup) {
	go func() {
		for {
			select {
			case failure := <-m.fch:
				m.Lock()
				m.failureRecvdCount++
				m.lastFailure = &failure
				m.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (m *mockFailureProducer) lastReceivedFailure() *model.Failure {
	m.Lock()
	defer m.Unlock()
	return m.lastFailure
}

func (m *mockFailureProducer) numberOfReceivedFailures() int {
	return m.failureRecvdCount
}
