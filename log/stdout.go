package log

import (
	"fmt"
	"log"
)

type StdOutLogger struct{}

func (s StdOutLogger) Debugf(format string, args ...interface{}) {
	fmt.Println("[DEBUG]: ", fmt.Sprintf(format, args...))
}

func (s StdOutLogger) Debug(args ...interface{}) {
	fmt.Println("[DEBUG]: ", args)
}

func (s StdOutLogger) Error(args ...interface{}) {
	fmt.Println("[ERROR]: ", args)
}

func (s StdOutLogger) Errorf(format string, args ...interface{}) {
	fmt.Println("[ERROR]: ", fmt.Sprintf(format, args...))
}

func (s StdOutLogger) Info(args ...interface{}) {
	fmt.Println("[INFO]: ", args)
}

func (s StdOutLogger) Infof(format string, args ...interface{}) {
	fmt.Println("[INFO]: ", fmt.Sprintf(format, args...))
}

func (s StdOutLogger) Panic(args ...interface{}) {
	log.Panic(args...)
}
