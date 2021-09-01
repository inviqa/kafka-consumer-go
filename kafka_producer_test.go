package consumer

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/log"
	"github.com/inviqa/kafka-consumer-go/test/saramatest"
)

func TestNewKafkaFailureProducer(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	sp := saramatest.NewMockSyncProducer()
	fch := make(<-chan data.Failure)
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
	if newKafkaFailureProducer(saramatest.NewMockSyncProducer(), make(<-chan data.Failure), nil) == nil {
		t.Errorf("expected a producer but got nil")
	}
}

func TestFailureProducer_ListenForFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sp := saramatest.NewMockSyncProducer()
	fch := make(chan data.Failure, 10)
	prod := newKafkaFailureProducer(sp, fch, log.NullLogger{})

	prod.listenForFailures(ctx, &sync.WaitGroup{})

	msg1 := []byte("hello")
	msg2 := []byte("world")

	f1 := data.Failure{
		Reason:        "something bad happened",
		Message:       msg1,
		TopicToSendTo: "test",
	}
	f2 := data.Failure{
		Reason:        "something bad happened",
		Message:       msg2,
		TopicToSendTo: "test2",
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
	fch := make(chan data.Failure, 10)
	prod := newKafkaFailureProducer(sp, fch, log.NullLogger{})

	prod.listenForFailures(ctx, &sync.WaitGroup{})

	failure := data.Failure{
		Reason:        "something else happened",
		Message:       []byte{},
		TopicToSendTo: "foo",
	}

	fch <- failure
	<-time.After(time.Millisecond * 5)
	cancel()
}
