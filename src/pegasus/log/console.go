package log

import (
	"os"
	"sync"
)

type ConsoleLogger struct {
	Level int
	mutex sync.Mutex
}

func (l *ConsoleLogger) init() error {
	return nil
}

func (l *ConsoleLogger) getLevel() int {
	return l.Level
}

func (l *ConsoleLogger) write(s string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	os.Stdout.WriteString(s)
}
