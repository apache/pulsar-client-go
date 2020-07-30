package pulsar

import (
	"github.com/sirupsen/logrus"
)

// logrusWrapper implements Logger interface
// based on underlying logrus.FieldLogger
type logrusWrapper struct {
	l logrus.FieldLogger
}

// NewLoggerWithLogrus creates a new logger which wraps
// the given logrus.Logger
func NewLoggerWithLogrus(logger *logrus.Logger) Logger {
	return &logrusWrapper{
		l: logger,
	}
}

func (l *logrusWrapper) SubLogger(fs Fields) Logger {
	return &logrusWrapper{
		l: l.l.WithFields(logrus.Fields(fs)),
	}
}

func (l *logrusWrapper) WithFields(fs Fields) Entry {
	return logrusEntry{
		e: l.l.WithFields(logrus.Fields(fs)),
	}
}

func (l *logrusWrapper) WithField(name string, value interface{}) Entry {
	return logrusEntry{
		e: l.l.WithField(name, value),
	}
}

func (l *logrusWrapper) Debug(args ...interface{}) {
	l.l.Debug(args)
}

func (l *logrusWrapper) Info(args ...interface{}) {
	l.l.Info(args)
}

func (l *logrusWrapper) Warn(args ...interface{}) {
	l.l.Warn(args)
}

func (l *logrusWrapper) Error(args ...interface{}) {
	l.l.Error(args)
}

func (l *logrusWrapper) Debugf(format string, args ...interface{}) {
	l.l.Debugf(format, args)
}

func (l *logrusWrapper) Infof(format string, args ...interface{}) {
	l.l.Infof(format, args)
}

func (l *logrusWrapper) Warnf(format string, args ...interface{}) {
	l.l.Warnf(format, args)
}

func (l *logrusWrapper) Errorf(format string, args ...interface{}) {
	l.l.Errorf(format, args)
}

type logrusEntry struct {
	e logrus.FieldLogger
}

func (l logrusEntry) WithFields(fs Fields) Entry {
	return logrusEntry{
		e: l.e.WithFields(logrus.Fields(fs)),
	}
}

func (l logrusEntry) WithField(name string, value interface{}) Entry {
	return logrusEntry{
		e: l.e.WithField(name, value),
	}
}

func (l logrusEntry) Debug(args ...interface{}) {
	l.e.Debug(args)
}

func (l logrusEntry) Info(args ...interface{}) {
	l.e.Info(args)
}

func (l logrusEntry) Warn(args ...interface{}) {
	l.e.Warn(args)
}

func (l logrusEntry) Error(args ...interface{}) {
	l.e.Error(args)
}

func (l logrusEntry) Debugf(format string, args ...interface{}) {
	l.e.Debugf(format, args)
}

func (l logrusEntry) Infof(format string, args ...interface{}) {
	l.e.Infof(format, args)
}

func (l logrusEntry) Warnf(format string, args ...interface{}) {
	l.e.Warnf(format, args)
}

func (l logrusEntry) Errorf(format string, args ...interface{}) {
	l.e.Errorf(format, args)
}
