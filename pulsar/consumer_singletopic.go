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

type singleTopicConsumer struct {
	*consumer
	messageCh chan ConsumerMessage
}

func newSingleTopicConsumer(client *client, options ConsumerOptions, topic string,
	messageCh chan ConsumerMessage, dlq *dlqRouter, disableForceTopicCreation bool) (*singleTopicConsumer, error) {
	consumer, err := newInternalConsumer(client, options, topic, messageCh, dlq, disableForceTopicCreation)
	if err != nil {
		return nil, err
	}

	return &singleTopicConsumer{
		consumer:  consumer,
		messageCh: messageCh,
	}, nil
}

func (c *singleTopicConsumer) Close() {
	c.consumer.Close()
	close(c.messageCh)
}
