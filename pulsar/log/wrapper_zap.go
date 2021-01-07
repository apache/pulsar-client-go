// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package log

import (
	"go.uber.org/zap"
)

// zapWrapper implements the pulsar Logger interface around an underlying zap.SugaredLogger
type zapWrapper struct {
	l *zap.SugaredLogger
}

// NewLoggerWithZap creates a new logger which wraps the given zap.SugaredLogger
func NewLoggerWithZap(logger *zap.SugaredLogger) Logger {
	return &zapWrapper{
		l: logger,
	}
}

// SubLogger creates a new logger with a set of fields injected
func (z *zapWrapper) SubLogger(fs Fields) Logger {
	return z.WithFields(fs).(Logger)
}

// WithFields injects a set of fields into a log entry.
// Due to the way zap works, this is equivalent to creating a new logger with the additional fields.
func (z *zapWrapper) WithFields(fs Fields) Entry {
	var kv []interface{}
	for k, v := range fs {
		kv = append(kv, k, v)
	}

	return &zapWrapper{
		l: z.l.With(kv...),
	}
}

// WithFields injects a field into a log entry.
// Due to the way zap works, this is equivalent to creating a new logger with the additional field.
func (z *zapWrapper) WithField(name string, value interface{}) Entry {
	return &zapWrapper{
		l: z.l.With(name, value),
	}
}

// WithError is a shorthand for WithField, using a field key of "error".
func (z *zapWrapper) WithError(err error) Entry {
	return z.WithField("error", err)
}

// Debug is a pass-through to zap.SugaredLogger.Debug()
func (z *zapWrapper) Debug(args ...interface{}) {
	z.l.Debug(args)
}

// Info is a pass-through to zap.SugaredLogger.Info()
func (z *zapWrapper) Info(args ...interface{}) {
	z.l.Info(args)
}

// Warn is a pass-through to zap.SugaredLogger.Warn()
func (z *zapWrapper) Warn(args ...interface{}) {
	z.l.Warn(args)
}

// Error is a pass-through to zap.SugaredLogger.Error()
func (z *zapWrapper) Error(args ...interface{}) {
	z.l.Error(args)
}

// Debugf is a pass-through to zap.SugaredLogger.Debugf()
func (z *zapWrapper) Debugf(format string, args ...interface{}) {
	z.l.Debugf(format, args)
}

// Infof is a pass-through to zap.SugaredLogger.Infof()
func (z *zapWrapper) Infof(format string, args ...interface{}) {
	z.l.Infof(format, args)
}

// Warnf is a pass-through to zap.SugaredLogger.Warnf()
func (z *zapWrapper) Warnf(format string, args ...interface{}) {
	z.l.Warnf(format, args)
}

// Errorf is a pass-through to zap.SugaredLogger.Errorf()
func (z *zapWrapper) Errorf(format string, args ...interface{}) {
	z.l.Errorf(format, args)
}
