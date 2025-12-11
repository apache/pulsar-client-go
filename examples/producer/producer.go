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

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	fileLogger := &lumberjack.Logger{
		Filename:   "/tmp/pulsar-go-sdk.log",
		MaxSize:    100,
		MaxBackups: 5,
		LocalTime:  true,
	}
	// this multiLogger prints logs to both stdout and fileLogger
	// if we only want to print logs to file, just pass fileLogger to slog.NewJSONHandler()
	multiLogger := io.MultiWriter(os.Stdout, fileLogger)
	logger := slog.New(slog.NewJSONHandler(multiLogger, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
		// If we are using a logrus logger or other third-party custom loggers,
		// we can skip the above slog logger initialization and pass the logger with its corresponding wrapper here.
		Logger: pulsarlog.NewLoggerWithSlog(logger),
	})
	if err != nil {
		logger.Error("create client err", "error", err)
		return
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "topic-1",
	})
	if err != nil {
		logger.Error("create producer err", "error", err)
		return
	}
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			logger.Error("send message error", "error", err)
			return
		} else {
			logger.Info("Published message", "msgId", msgId.String())
		}
	}
}
