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
	"errors"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	log "github.com/sirupsen/logrus"
)

type dlqRouter struct {
	client    Client
	producer  Producer
	policy    *DLQPolicy
	messageCh chan ConsumerMessage
	closeCh   chan interface{}
	log       *log.Entry
}

func newDlqRouter(client Client, policy *DLQPolicy) (*dlqRouter, error) {
	r := &dlqRouter{
		client: client,
		policy: policy,
	}

	if policy != nil {
		if policy.MaxDeliveries <= 0 {
			return nil, errors.New("DLQPolicy.MaxDeliveries needs to be > 0")
		}

		if policy.Topic == "" {
			return nil, errors.New("DLQPolicy.Topic needs to be set to a valid topic name")
		}

		r.messageCh = make(chan ConsumerMessage)
		r.closeCh = make(chan interface{})
		r.log = log.WithField("dlq-topic", policy.Topic)
		go r.run()
	}
	return r, nil
}

func (r *dlqRouter) shouldSendToDlq(cm *ConsumerMessage) bool {
	if r.policy == nil {
		return false
	}

	msg := cm.Message.(*message)
	r.log.WithField("count", msg.redeliveryCount).
		WithField("max", r.policy.MaxDeliveries).
		WithField("msgId", msg.msgID).
		Debug("Should route to DLQ?")

	// We use >= here because we're comparing the number of re-deliveries with
	// the number of deliveries. So:
	//  * the user specifies that wants to process a message up to 10 times.
	//  * the first time, the redeliveryCount == 0, then 1 and so on
	//  * when we receive the message and redeliveryCount == 10, it means
	//    that the application has already got (and Nack())  the message 10
	//    times, so this time we should just go to DLQ.
	return cm.Message.(*message).redeliveryCount >= r.policy.MaxDeliveries
}

func (r *dlqRouter) Chan() chan ConsumerMessage {
	return r.messageCh
}

func (r *dlqRouter) run() {
	for {
		select {
		case cm := <-r.messageCh:
			r.log.WithField("msgID", cm.ID()).Debug("Got message for DLQ")
			producer := r.getProducer()

			msg := cm.Message.(*message)
			msgID := msg.ID()
			eventTime := msg.EventTime()
			producer.SendAsync(context.Background(), &ProducerMessage{
				Payload:             msg.Payload(),
				Key:                 msg.Key(),
				Properties:          msg.Properties(),
				EventTime:           &eventTime,
				ReplicationClusters: msg.replicationClusters,
			}, func(MessageID, *ProducerMessage, error) {
				r.log.WithField("msgID", msgID).Debug("Sent message to DLQ")
				cm.Consumer.AckID(msgID)
			})

		case <-r.closeCh:
			if r.producer != nil {
				r.producer.Close()
			}
			r.log.Debug("Closed DLQ router")
			return
		}
	}
}

func (r *dlqRouter) close() {
	if r.closeCh != nil {
		r.closeCh <- nil
	}
}

func (r *dlqRouter) getProducer() Producer {
	if r.producer != nil {
		// Producer was already initialized
		return r.producer
	}

	// Retry to create producer indefinitely
	backoff := &internal.Backoff{}
	for {
		producer, err := r.client.CreateProducer(ProducerOptions{
			Topic:                   r.policy.Topic,
			CompressionType:         LZ4,
			BatchingMaxPublishDelay: 100 * time.Millisecond,
		})

		if err != nil {
			r.log.WithError(err).Error("Failed to create DLQ producer")
			time.Sleep(backoff.Next())
			continue
		} else {
			r.producer = producer
			return producer
		}
	}
}
