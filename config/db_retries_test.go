package config

import (
	"testing"

	"github.com/go-test/deep"

	"github.com/inviqa/kafka-consumer-go/data/retry/model"
)

func TestDBRetries_MakeRetryErrored(t *testing.T) {
	retries := DBRetries{
		"foo": []*DBTopicRetry{
			{
				Interval: 100,
				Sequence: 1,
				Key:      "foo",
			},
			{
				Interval: 200,
				Sequence: 2,
				Key:      "foo",
			},
		},
	}

	t.Run("it marks retry as errored", func(t *testing.T) {
		retry := model.Retry{Topic: "foo"}
		exp := model.Retry{
			Topic:    "foo",
			Attempts: 1,
			Errored:  true,
		}

		if diff := deep.Equal(exp, retries.MakeRetryErrored(retry)); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("it marks retry as dead-lettered", func(t *testing.T) {
		retry := model.Retry{Topic: "foo", Attempts: 1}
		exp := model.Retry{
			Topic:        "foo",
			Attempts:     2,
			Errored:      true,
			Deadlettered: true,
		}

		if diff := deep.Equal(exp, retries.MakeRetryErrored(retry)); diff != nil {
			t.Error(diff)
		}
	})
}

func TestDBRetries_MakeRetrySuccessful(t *testing.T) {
	retries := DBRetries{}

	t.Run("it marks retry as successful", func(t *testing.T) {
		retry := model.Retry{Topic: "bar"}
		exp := model.Retry{
			Topic:    "bar",
			Attempts: 1,
		}

		if diff := deep.Equal(exp, retries.MakeRetrySuccessful(retry)); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("it marks previously errored retry as successful", func(t *testing.T) {
		retry := model.Retry{
			Topic:   "bar",
			Errored: true,
		}
		exp := model.Retry{
			Topic:    "bar",
			Attempts: 1,
		}

		if diff := deep.Equal(exp, retries.MakeRetrySuccessful(retry)); diff != nil {
			t.Error(diff)
		}
	})
}
