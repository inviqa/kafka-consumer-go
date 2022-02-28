//go:build integration
// +build integration

package kafka

type TestMessage struct {
	Type     string   `json:"type"`
	Data     struct{} `json:"data"`
	XEventId string   `json:"event_id"`
}
