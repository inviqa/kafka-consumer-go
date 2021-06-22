package consumer

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/inviqa/kafka-consumer-go/test"
)

func TestNewFailureProducer(t *testing.T) {
	if NewFailureProducer(test.NewMockSyncProducer(), make(<-chan Failure), NullLogger{}) == nil {
		t.Errorf("expected a producer but got nil")
	}
}

func TestNewFailureProducer_WithNilLogger(t *testing.T) {
	if NewFailureProducer(test.NewMockSyncProducer(), make(<-chan Failure), nil) == nil {
		t.Errorf("expected a producer but got nil")
	}
}

func TestFailureProducer_ListenForFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sp := test.NewMockSyncProducer()
	fch := make(chan Failure, 10)
	prod := NewFailureProducer(sp, fch, NullLogger{})

	prod.ListenForFailures(ctx, &sync.WaitGroup{})

	msg1 := []byte("hello")
	msg2 := []byte("world")

	f1 := Failure{
		Reason:        "something bad happened",
		Message:       msg1,
		TopicToSendTo: "test",
	}
	f2 := Failure{
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
	sp := test.NewMockSyncProducer()
	sp.ReturnErrorOnSend()
	fch := make(chan Failure, 10)
	prod := NewFailureProducer(sp, fch, NullLogger{})

	prod.ListenForFailures(ctx, &sync.WaitGroup{})

	failure := Failure{
		Reason:        "something else happened",
		Message:       []byte{},
		TopicToSendTo: "foo",
	}

	fch <- failure
	<-time.After(time.Millisecond * 5)
	cancel()
}
