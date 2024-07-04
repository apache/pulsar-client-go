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
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlog(t *testing.T) {
	var logBuffer bytes.Buffer
	logMessage := "info message"
	loggerSlog := slog.New(slog.NewJSONHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelInfo}))
	pulsarLogger := NewLoggerWithSlog(loggerSlog)

	pulsarLogger.Info(logMessage)

	logOutputSlog := logBuffer.String()
	slogLogLines := strings.Split(strings.TrimSpace(logOutputSlog), "\n")
	var slogLine map[string]interface{}
	err := json.Unmarshal([]byte(slogLogLines[0]), &slogLine)

	require.Len(t, slogLogLines, 1)
	require.NoError(t, err)
	require.Equal(t, slog.LevelInfo.String(), slogLine[slog.LevelKey])
	require.Equal(t, logMessage, slogLine[slog.MessageKey])
}
