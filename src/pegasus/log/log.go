package log

import (
	"fmt"
	"time"
)

type Logger interface {
	init() error
	write(s string)
	getLevel() int
}

var loggers = []Logger{}

func RegisterLogger(l Logger) error {
	if err := l.init(); err != nil {
		return err
	}
	loggers = append(loggers, l)
	return nil
}

const (
	LevelDebug = iota
	LevelInfo
	LevelError
	LevelAlloff
)

func GetLogLevelStr(level int) string {
	switch level {
	case LevelDebug:
		return "Debug"
	case LevelInfo:
		return "Info"
	case LevelError:
		return "Error"
	case LevelAlloff:
		return "LogOFF"
	default:
		return "Unknown"
	}
}

func Debug(format string, args ...interface{}) {
	writeLoggers(LevelDebug, format, args...)
}

func Info(format string, args ...interface{}) {
	writeLoggers(LevelInfo, format, args...)
}

func Error(format string, args ...interface{}) {
	writeLoggers(LevelError, format, args...)
}

func buildLogMsg(level int, format string, args ...interface{}) string {
	ts := time.Now().Format("2006-01-02 15:04:05.999")
	l := GetLogLevelStr(level)
	s := fmt.Sprintf(format, args...)
	return fmt.Sprintf("%s %s %s\n", ts, l, s)
}

func writeLoggers(level int, format string, args ...interface{}) {
	for _, l := range loggers {
		if level >= l.getLevel() {
			s := buildLogMsg(level, format, args...)
			l.write(s)
		}
	}
}
