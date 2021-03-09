package consumer

type Failure struct {
	Reason        string
	TopicToSendTo string
	Message       []byte
}
