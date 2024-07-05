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

//go:build go1.21

package log

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlogLevels(t *testing.T) {
	testCases := []struct {
		level       slog.Level
		logFunction func(logger Logger, msg string)
	}{
		{slog.LevelDebug, func(logger Logger, msg string) { logger.Debug(msg) }},
		{slog.LevelInfo, func(logger Logger, msg string) { logger.Info(msg) }},
		{slog.LevelWarn, func(logger Logger, msg string) { logger.Warn(msg) }},
		{slog.LevelError, func(logger Logger, msg string) { logger.Error(msg) }},
	}

	for _, tc := range testCases {
		t.Run(tc.level.String(), func(t *testing.T) {
			var logBuffer bytes.Buffer
			logMessage := "test message"
			loggerSlog := slog.New(slog.NewJSONHandler(&logBuffer, &slog.HandlerOptions{Level: tc.level}))
			pulsarLogger := NewLoggerWithSlog(loggerSlog)

			tc.logFunction(pulsarLogger, logMessage)

			logOutputSlog := logBuffer.String()
			verifyLogOutput(t, logOutputSlog, tc.level.String(), logMessage)
		})
	}
}

func TestSlogPrintMethods(t *testing.T) {
	testCases := []struct {
		level       slog.Level
		logFunction func(logger Logger, format string, args ...any)
	}{
		{
			level: slog.LevelDebug,
			logFunction: func(logger Logger, format string, args ...any) {
				logger.Debugf(format, args...)
			},
		},
		{
			level: slog.LevelInfo,
			logFunction: func(logger Logger, format string, args ...any) {
				logger.Infof(format, args...)
			},
		},
		{
			level: slog.LevelWarn,
			logFunction: func(logger Logger, format string, args ...any) {
				logger.Warnf(format, args...)
			},
		},
		{
			level: slog.LevelError,
			logFunction: func(logger Logger, format string, args ...any) {
				logger.Errorf(format, args...)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.level.String()+"f", func(t *testing.T) {
			var logBuffer bytes.Buffer
			logMessage := "formatted message for %s"
			expectedMessage := "formatted message for " + tc.level.String()
			loggerSlog := slog.New(slog.NewJSONHandler(&logBuffer, &slog.HandlerOptions{Level: tc.level}))
			pulsarLogger := NewLoggerWithSlog(loggerSlog)

			tc.logFunction(pulsarLogger, logMessage, tc.level.String())

			logOutputSlog := logBuffer.String()
			verifyLogOutput(t, logOutputSlog, tc.level.String(), expectedMessage)
		})
	}
}

func TestSlogWrapperWithMethods(t *testing.T) {
	testCases := []struct {
		name           string
		level          slog.Level
		testMessage    string
		setupLogger    func(logger Logger) Entry
		expectedFields Fields
	}{
		{
			name:        "WithField",
			level:       slog.LevelInfo,
			testMessage: "Message with field",
			setupLogger: func(logger Logger) Entry {
				return logger.WithField("key", "value")
			},
			expectedFields: Fields{"key": "value"},
		},
		{
			name:        "WithFields",
			level:       slog.LevelInfo,
			testMessage: "Message with multiple fields",
			setupLogger: func(logger Logger) Entry {
				return logger.WithFields(Fields{"key1": "value1", "key2": "value2"})
			},
			expectedFields: Fields{"key1": "value1", "key2": "value2"},
		},
		{
			name:        "WithError",
			level:       slog.LevelInfo,
			testMessage: "Message with error field",
			setupLogger: func(logger Logger) Entry {
				return logger.WithError(errors.New("test error"))
			},
			expectedFields: Fields{"error": "test error"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var logBuffer bytes.Buffer
			loggerSlog := slog.New(slog.NewJSONHandler(&logBuffer, &slog.HandlerOptions{Level: tc.level}))
			pulsarLogger := NewLoggerWithSlog(loggerSlog)

			entry := tc.setupLogger(pulsarLogger)
			switch tc.level {
			case slog.LevelDebug:
				entry.Debug(tc.testMessage)
			case slog.LevelInfo:
				entry.Info(tc.testMessage)
			case slog.LevelWarn:
				entry.Warn(tc.testMessage)
			case slog.LevelError:
				entry.Error(tc.testMessage)
			default:
				t.Errorf("Unsupported log level: %v", tc.level)
			}

			verifyLogOutput(t, logBuffer.String(), tc.level.String(), tc.testMessage, tc.expectedFields)
		})
	}
}

func verifyLogOutput(t *testing.T, logOutput, expectedLevel, expectedMessage string, expectedFields ...Fields) {
	logLines := strings.Split(strings.TrimSpace(logOutput), "\n")
	require.Len(t, logLines, 1, "There should be exactly one log line.")

	var logEntry map[string]interface{}
	err := json.Unmarshal([]byte(logLines[0]), &logEntry)
	require.NoError(t, err, "Log entry should be valid JSON.")
	require.Equal(t, expectedLevel, logEntry["level"], "Log level should match expected level.")
	require.Contains(t, logEntry["msg"], expectedMessage, "Log message should contain expected message.")

	if len(expectedFields) > 0 {
		for key, expectedValue := range expectedFields[0] {
			actualValue, ok := logEntry[key]
			require.True(t, ok, fmt.Sprintf("Expected key '%s' to be present in the log entry", key))
			require.Equal(t, expectedValue, actualValue, fmt.Sprintf("Value for key '%s' should match the expected value", key))
		}
	}
}
