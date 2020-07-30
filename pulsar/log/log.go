package log

// DefaultNopLogger returns a nop logger.
func DefaultNopLogger() Logger {
	return nopLogger{}
}

type nopLogger struct{}

func (l nopLogger) SubLogger(fields Fields) Logger                 { return l }
func (l nopLogger) WithFields(fields Fields) Entry                 { return nopEntry{} }
func (l nopLogger) WithField(name string, value interface{}) Entry { return nopEntry{} }
func (l nopLogger) Debug(args ...interface{})                      {}
func (l nopLogger) Info(args ...interface{})                       {}
func (l nopLogger) Warn(args ...interface{})                       {}
func (l nopLogger) Error(args ...interface{})                      {}
func (l nopLogger) Debugf(format string, args ...interface{})      {}
func (l nopLogger) Infof(format string, args ...interface{})       {}
func (l nopLogger) Warnf(format string, args ...interface{})       {}
func (l nopLogger) Errorf(format string, args ...interface{})      {}

type nopEntry struct{}

func (e nopEntry) WithFields(fields Fields) Entry                 { return nopEntry{} }
func (e nopEntry) WithField(name string, value interface{}) Entry { return nopEntry{} }

func (e nopEntry) Debug(args ...interface{})                 {}
func (e nopEntry) Info(args ...interface{})                  {}
func (e nopEntry) Warn(args ...interface{})                  {}
func (e nopEntry) Error(args ...interface{})                 {}
func (e nopEntry) Debugf(format string, args ...interface{}) {}
func (e nopEntry) Infof(format string, args ...interface{})  {}
func (e nopEntry) Warnf(format string, args ...interface{})  {}
func (e nopEntry) Errorf(format string, args ...interface{}) {}
