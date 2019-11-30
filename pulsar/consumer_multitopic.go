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
	"fmt"
	"sync"

	pkgerrors "github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

type multiTopicConsumer struct {
	options ConsumerOptions

	messageCh chan ConsumerMessage

	consumers map[string]Consumer

	closeOnce sync.Once
	closeCh   chan struct{}

	log *log.Entry
}

func newMultiTopicConsumer(client *client, options ConsumerOptions, topics []string,
	messageCh chan ConsumerMessage) (Consumer, error) {
	mtc := &multiTopicConsumer{
		options:   options,
		messageCh: messageCh,
		consumers: make(map[string]Consumer, len(topics)),
		closeCh:   make(chan struct{}),
		log:       &log.Entry{},
	}

	var errs error
	for ce := range subscriber(client, topics, options, messageCh) {
		if ce.err != nil {
			errs = pkgerrors.Wrapf(ce.err, "unable to subscribe to topic=%s", ce.topic)
		} else {
			mtc.consumers[ce.topic] = ce.consumer
		}
	}

	if errs != nil {
		for _, c := range mtc.consumers {
			c.Close()
		}
		return nil, errs
	}

	return mtc, nil
}

func (c *multiTopicConsumer) Subscription() string {
	return c.options.SubscriptionName
}

func (c *multiTopicConsumer) Unsubscribe() error {
	var errs error
	for t, consumer := range c.consumers {
		if err := consumer.Unsubscribe(); err != nil {
			msg := fmt.Sprintf("unable to unsubscribe from topic=%s subscription=%s",
				t, c.Subscription())
			errs = pkgerrors.Wrap(err, msg)
		}
	}
	return errs
}

func (c *multiTopicConsumer) Receive(ctx context.Context) (message Message, err error) {
	for {
		select {
		case <-c.closeCh:
			return nil, ErrConsumerClosed
		case cm, ok := <-c.messageCh:
			if !ok {
				return nil, ErrConsumerClosed
			}
			return cm.Message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Messages
func (c *multiTopicConsumer) Chan() <-chan ConsumerMessage {
	return c.messageCh
}

// Ack the consumption of a single message
func (c *multiTopicConsumer) Ack(msg Message) {
	c.AckID(msg.ID())
}

// Ack the consumption of a single message, identified by its MessageID
func (c *multiTopicConsumer) AckID(msgID MessageID) {
	mid, ok := msgID.(*messageID)
	if !ok {
		c.log.Warnf("invalid message id type")
		return
	}

	if mid.consumer == nil {
		c.log.Warnf("unable to ack messageID=%+v can not determine topic", msgID)
		return
	}

	mid.Ack()
}

func (c *multiTopicConsumer) Nack(msg Message) {
	c.AckID(msg.ID())
}

func (c *multiTopicConsumer) NackID(msgID MessageID) {
	mid, ok := msgID.(*messageID)
	if !ok {
		c.log.Warnf("invalid message id type")
		return
	}

	if mid.consumer == nil {
		c.log.Warnf("unable to nack messageID=%+v can not determine topic", msgID)
		return
	}

	mid.Nack()
}

func (c *multiTopicConsumer) Close() {
	c.closeOnce.Do(func() {
		var wg sync.WaitGroup
		wg.Add(len(c.consumers))
		for _, con := range c.consumers {
			go func(consumer Consumer) {
				defer wg.Done()
				consumer.Close()
			}(con)
		}
		wg.Wait()
		close(c.closeCh)
	})
}
