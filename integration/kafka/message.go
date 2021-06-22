package kafka

type TestMessage struct {
	Type     string   `json:"type"`
	Data     TestData `json:"data"`
	XEventId string   `json:"event_id"`
}

type TestData struct{}
