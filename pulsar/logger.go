package pulsar

// Logger and Entry interfaces are inspired by sirupsen/logrus

// Fields type, used to pass to `WithFields`.
type Fields map[string]interface{}

// Logger describes the interface that must be implemeted by all loggers.
type Logger interface {
	SubLogger(fields Fields) Logger

	WithFields(fields Fields) Entry
	WithField(name string, value interface{}) Entry

	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// Entry describes the interface for the logger entry.
type Entry interface {
	WithFields(fields Fields) Entry
	WithField(name string, value interface{}) Entry

	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}
