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

import "github.com/sirupsen/logrus"

// NewLoggerWithLogrus creates a new logger which wraps
// the given logrus.FieldLogger.
func NewLoggerWithLogrus(logger logrus.FieldLogger) Logger {
	return logrusWrapper{
		log: logger,
	}
}

type logrusWrapper struct {
	log logrus.FieldLogger
}

func (l logrusWrapper) WithError(err error) Logger {
	return logrusWrapper{
		log: l.log.WithError(err),
	}
}

func (l logrusWrapper) WithFields(fs Fields) Logger {
	return logrusWrapper{
		log: l.log.WithFields(logrus.Fields(fs)),
	}
}

func (l logrusWrapper) WithField(name string, value interface{}) Logger {
	return logrusWrapper{
		log: l.log.WithField(name, value),
	}
}

func (l logrusWrapper) Debug(args ...interface{})                 { l.log.Debug(args) }
func (l logrusWrapper) Info(args ...interface{})                  { l.log.Info(args) }
func (l logrusWrapper) Warn(args ...interface{})                  { l.log.Warn(args) }
func (l logrusWrapper) Error(args ...interface{})                 { l.log.Error(args) }
func (l logrusWrapper) Debugf(format string, args ...interface{}) { l.log.Debugf(format, args) }
func (l logrusWrapper) Infof(format string, args ...interface{})  { l.log.Infof(format, args) }
func (l logrusWrapper) Warnf(format string, args ...interface{})  { l.log.Warnf(format, args) }
func (l logrusWrapper) Errorf(format string, args ...interface{}) { l.log.Errorf(format, args) }
