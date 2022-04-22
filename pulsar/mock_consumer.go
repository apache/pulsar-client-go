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

package pulsar

import (
	"context"
	"time"
)

func NewMockConsumer() Consumer {
	return &mockConsumer{}
}

type mockConsumer struct {
}

func (c *mockConsumer) Subscription() string {
	return ""
}

func (c *mockConsumer) Unsubscribe() error {
	return nil
}

func (c *mockConsumer) Receive(ctx context.Context) (message Message, err error) {
	return nil, nil
}

func (c *mockConsumer) Chan() <-chan ConsumerMessage {
	return nil
}

func (c *mockConsumer) Ack(msg Message) {}

func (c *mockConsumer) AckID(msgID MessageID) {}

func (c *mockConsumer) ReconsumeLater(msg Message, delay time.Duration) {}

func (c *mockConsumer) Nack(msg Message) {}

func (c *mockConsumer) NackID(msgID MessageID) {}

func (c *mockConsumer) Close() {}

func (c *mockConsumer) Seek(msgID MessageID) error {
	return nil
}

func (c *mockConsumer) SeekByTime(time time.Time) error {
	return nil
}

func (c *mockConsumer) Name() string {
	return ""
}

func (c *mockConsumer) lastDequeuedMsg(msgID MessageID) error {
	return nil
}

func (c *mockConsumer) messagesInQueue() int {
	return 0
}

func (c *mockConsumer) hasMessages() (bool, error) {
	return false, nil
}
