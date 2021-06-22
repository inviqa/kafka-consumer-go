package consumer

type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
}

type NullLogger struct{}

func (n NullLogger) Debugf(format string, args ...interface{}) {
}

func (n NullLogger) Debug(args ...interface{}) {
}

func (n NullLogger) Error(args ...interface{}) {
}

func (n NullLogger) Errorf(format string, args ...interface{}) {
}

func (n NullLogger) Info(args ...interface{}) {
}

func (n NullLogger) Infof(format string, args ...interface{}) {
}
