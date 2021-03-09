package consumer

type Logger interface {
	Debugf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
}

type nullLogger struct{}

func (n nullLogger) Debugf(format string, args ...interface{}) {
}

func (n nullLogger) Error(args ...interface{}) {
}

func (n nullLogger) Errorf(format string, args ...interface{}) {
}

func (n nullLogger) Info(args ...interface{}) {
}

func (n nullLogger) Infof(format string, args ...interface{}) {
}
