package consumer

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/log"
	"github.com/inviqa/kafka-consumer-go/test/saramatest"
)

func TestNewKafkaFailureProducer(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	sp := saramatest.NewMockSyncProducer()
	fch := make(<-chan model.Failure)
	logger := log.NullLogger{}

	exp := &kafkaFailureProducer{
		producer: sp,
		fch:      fch,
		logger:   logger,
	}

	got := newKafkaFailureProducer(sp, fch, logger)

	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

func TestNewFailureProducer_WithNilLogger(t *testing.T) {
	if newKafkaFailureProducer(saramatest.NewMockSyncProducer(), make(<-chan model.Failure), nil) == nil {
		t.Errorf("expected a producer but got nil")
	}
}

func TestFailureProducer_ListenForFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sp := saramatest.NewMockSyncProducer()
	fch := make(chan model.Failure, 10)
	prod := newKafkaFailureProducer(sp, fch, log.NullLogger{})

	prod.listenForFailures(ctx, &sync.WaitGroup{})

	msg1 := []byte("hello")
	msg2 := []byte("world")

	f1 := model.Failure{
		Reason:    "something bad happened",
		Message:   msg1,
		NextTopic: "test",
	}
	f2 := model.Failure{
		Reason:    "something bad happened",
		Message:   msg2,
		NextTopic: "test2",
	}

	fch <- f1
	fch <- f2
	<-time.After(time.Millisecond * 5)
	cancel()

	act1 := sp.GetLastMessageReceived("test")
	act2 := sp.GetLastMessageReceived("test2")
	if bytes.Compare(msg1, act1) != 0 {
		t.Errorf("expected '%s' to be published, but got '%s'", msg1, act1)
	}
	if bytes.Compare(msg2, act2) != 0 {
		t.Errorf("expected '%s' to be published, but got '%s'", msg2, act2)
	}
}

func TestFailureProducer_ListenForFailuresWithProducerError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sp := saramatest.NewMockSyncProducer()
	sp.ReturnErrorOnSend()
	fch := make(chan model.Failure, 10)
	prod := newKafkaFailureProducer(sp, fch, log.NullLogger{})

	prod.listenForFailures(ctx, &sync.WaitGroup{})

	failure := model.Failure{
		Reason:    "something else happened",
		Message:   []byte{},
		NextTopic: "foo",
	}

	fch <- failure
	<-time.After(time.Millisecond * 5)
	cancel()
}
