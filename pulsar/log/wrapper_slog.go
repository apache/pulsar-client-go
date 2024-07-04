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
	"fmt"
	"log/slog"
)

type slogWrapper struct {
	logger *slog.Logger
}

func (s *slogWrapper) Debug(args ...interface{}) {
	message := s.tryDetermineMessage(args)
	s.logger.Debug(message)
}

func (s *slogWrapper) Info(args ...interface{}) {
	message := s.tryDetermineMessage(args)
	s.logger.Info(message)
}

func (s *slogWrapper) Error(args ...interface{}) {
	message := s.tryDetermineMessage(args)
	s.logger.Error(message)
}

func (s *slogWrapper) Warn(args ...interface{}) {
	message := s.tryDetermineMessage(args)
	s.logger.Warn(message)
}

func (s *slogWrapper) Debugf(format string, args ...interface{}) {
	s.logger.Debug(fmt.Sprintf(format, args...))
}

func (s *slogWrapper) Infof(format string, args ...interface{}) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *slogWrapper) Warnf(format string, args ...interface{}) {
	s.logger.Warn(fmt.Sprintf(format, args...))
}

func (s *slogWrapper) Errorf(format string, args ...interface{}) {
	s.logger.Error(fmt.Sprintf(format, args...))
}

func (s *slogWrapper) SubLogger(fields Fields) Logger {
	return &slogWrapper{
		logger: s.logger.With(pulsarFieldsToKVSlice(fields)...),
	}
}

func (s *slogWrapper) WithError(err error) Entry {
	return s.WithField("error", err)
}

func (s *slogWrapper) WithField(name string, value interface{}) Entry {
	return &slogWrapper{
		logger: s.logger.With(name, value),
	}
}

func (s *slogWrapper) WithFields(fields Fields) Entry {
	return &slogWrapper{
		logger: s.logger.With(pulsarFieldsToKVSlice(fields)...),
	}
}

func NewLoggerWithSlog(logger *slog.Logger) Logger {
	return &slogWrapper{
		logger: logger,
	}
}

func pulsarFieldsToKVSlice(f Fields) []interface{} {
	ret := make([]interface{}, 0, len(f)*2)
	for k, v := range f {
		ret = append(ret, k, v)
	}
	return ret
}

func (s *slogWrapper) tryDetermineMessage(value ...interface{}) string {
	data, ok := value[0].(string)
	if ok {
		return data
	}

	if _, ok := value[0].([]interface{}); ok {
		for _, item := range value {
			return s.tryDetermineMessage(item.([]interface{})...)
		}
	}

	return ""
}
