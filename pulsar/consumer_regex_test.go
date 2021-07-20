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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

func TestFilterTopics(t *testing.T) {
	t.Run("MatchingAll", runFilterTopicsMatchingAllTopics)
	t.Run("MatchingFoo", runFilterTopicsMatchingFoo)
}

func runFilterTopicsMatchingAllTopics(t *testing.T) {
	regex := regexp.MustCompile("public/default/.*")
	topics := []string{
		"persistent://public/default/my-topic-1",
	}

	matching := filterTopics(topics, regex)
	assert.Equal(t, topics, matching)

	// test partitions
	topics = []string{
		"persistent://public/default/my-topic-partition-0",
		"persistent://public/default/my-topic-partition-1",
		"persistent://public/default/my-topic-partition-3",
	}
	matching = filterTopics(topics, regex)
	assert.Equal(t, 1, len(matching))
	assert.Equal(t, "persistent://public/default/my-topic", matching[0])
}

func runFilterTopicsMatchingFoo(t *testing.T) {
	regex := regexp.MustCompile("public/foo/foo.*")
	topics := []string{
		"persistent://public/foo/foo",
	}
	matching := filterTopics(topics, regex)
	assert.Equal(t, topics, matching)

	topics = []string{
		"persistent://public/foo/foo",
		"persistent://public/foo/fo-my-topic",
	}
	matching = filterTopics(topics, regex)
	assert.Equal(t, 1, len(matching))
	assert.Equal(t, topics[0], matching[0])

	topics = []string{
		"persistent://public/foo/foo",
		"persistent://public/foo/foobar",
		"persistent://public/foo/foo-my-topic",
	}
	matching = filterTopics(topics, regex)
	assert.Equal(t, 3, len(matching))
	assert.Equal(t, topics, matching)

	topics = []string{
		"persistent://public/foo/my-topic",
		"persistent://public/foo/foobar-partition-0",
		"persistent://public/foo/foobar-partition-1",
	}
	matching = filterTopics(topics, regex)
	assert.Equal(t, 1, len(matching))
	assert.Equal(t, "persistent://public/foo/foobar", matching[0])
}

func TestTopicsDiff(t *testing.T) {
	topics1 := []string{
		"my-topic-a",
	}
	assert.Equal(t, topics1, topicsDiff(topics1, []string{}))

	topics1 = []string{
		"my-topic-a",
	}
	topics2 := []string{
		"my-topic-a",
	}
	assert.Equal(t, []string{}, topicsDiff(topics1, topics2))

	topics1 = []string{
		"my-topic-a",
		"my-topic-b",
	}
	topics2 = []string{
		"my-topic-a",
	}
	assert.Equal(t, []string{"my-topic-b"}, topicsDiff(topics1, topics2))

	topics1 = []string{
		"my-topic-a",
	}
	topics2 = []string{
		"my-topic-a",
		"my-topic-b",
	}
	assert.Equal(t, []string{}, topicsDiff(topics1, topics2))
}

func runWithClientNamespace(fn func(*testing.T, Client, string)) func(*testing.T) {
	return func(t *testing.T) {
		ns := fmt.Sprintf("public/%s", generateRandomName())
		err := createNamespace(ns, anonymousNamespacePolicy())
		if err != nil {
			t.Fatal(err)
		}
		c, err := NewClient(ClientOptions{
			URL: serviceURL,
		})
		if err != nil {
			t.Fatal(err)
		}
		fn(t, c, ns)
	}
}

func TestRegexConsumerDiscover(t *testing.T) {
	t.Run("PatternAll", runWithClientNamespace(runRegexConsumerDiscoverPatternAll))
	t.Run("PatternFoo", runWithClientNamespace(runRegexConsumerDiscoverPatternFoo))
}

func runRegexConsumerDiscoverPatternAll(t *testing.T, c Client, namespace string) {
	tn, _ := internal.ParseTopicName(fmt.Sprintf("persistent://%s/.*", namespace))
	pattern := regexp.MustCompile(fmt.Sprintf("%s/.*", namespace))
	opts := ConsumerOptions{
		SubscriptionName:    "regex-sub",
		AutoDiscoveryPeriod: 5 * time.Minute,
	}

	dlq, _ := newDlqRouter(c.(*client), nil, log.DefaultNopLogger())
	rlq, _ := newRetryRouter(c.(*client), nil, false, log.DefaultNopLogger())
	consumer, err := newRegexConsumer(c.(*client), opts, tn, pattern, make(chan ConsumerMessage, 1), dlq, rlq)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	rc := consumer.(*regexConsumer)

	// trigger discovery
	rc.discover()

	consumers := cloneConsumers(rc)
	assert.Equal(t, 0, len(consumers))

	topic := namespace + "/my-topic"
	// create a topic
	err = createTopic(topic)
	if err != nil {
		t.Fatal(err)
	}
	rc.discover()
	time.Sleep(2000 * time.Millisecond)

	consumers = cloneConsumers(rc)
	assert.Equal(t, 1, len(consumers))
}

func runRegexConsumerDiscoverPatternFoo(t *testing.T, c Client, namespace string) {
	tn, _ := internal.ParseTopicName(fmt.Sprintf("persistent://%s/foo-*", namespace))
	pattern := regexp.MustCompile(fmt.Sprintf("%s/foo-*", namespace))
	opts := ConsumerOptions{
		SubscriptionName:    "regex-sub",
		AutoDiscoveryPeriod: 5 * time.Minute,
	}

	dlq, _ := newDlqRouter(c.(*client), nil, log.DefaultNopLogger())
	rlq, _ := newRetryRouter(c.(*client), nil, false, log.DefaultNopLogger())
	consumer, err := newRegexConsumer(c.(*client), opts, tn, pattern, make(chan ConsumerMessage, 1), dlq, rlq)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	rc := consumer.(*regexConsumer)

	// trigger discovery
	rc.discover()

	consumers := cloneConsumers(rc)
	assert.Equal(t, 0, len(consumers))

	// create a topic not in the regex
	myTopic := namespace + "/my-topic"
	err = createTopic(myTopic)
	if err != nil {
		t.Fatal(err)
	}
	defer deleteTopic(myTopic)

	rc.discover()
	time.Sleep(2000 * time.Millisecond)

	consumers = cloneConsumers(rc)
	assert.Equal(t, 0, len(consumers))

	// create a topic not in the regex
	fooTopic := namespace + "/foo-topic"
	err = createTopic(fooTopic)
	if err != nil {
		t.Fatal(err)
	}

	rc.discover()
	time.Sleep(2000 * time.Millisecond)

	consumers = cloneConsumers(rc)
	assert.Equal(t, 1, len(consumers))
}

func TestRegexConsumer(t *testing.T) {
	t.Run("MatchOneTopic", runWithClientNamespace(runRegexConsumerMatchOneTopic))
	t.Run("AddTopic", runWithClientNamespace(runRegexConsumerAddMatchingTopic))
	t.Run("RemoveTopic", runWithClientNamespace(runRegexConsumerRemoveMatchingTopic))
}

func runRegexConsumerMatchOneTopic(t *testing.T, c Client, namespace string) {
	topicNotInRegex := fmt.Sprintf("%s/my-topic", namespace)
	topicInRegex := fmt.Sprintf("%s/foo-topic", namespace)

	p1, err := c.CreateProducer(ProducerOptions{
		Topic:           topicNotInRegex,
		DisableBatching: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p1.Close()

	p2, err := c.CreateProducer(ProducerOptions{
		Topic:           topicInRegex,
		DisableBatching: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	topicsPattern := fmt.Sprintf("persistent://%s/foo.*", namespace)
	opts := ConsumerOptions{
		TopicsPattern:    topicsPattern,
		SubscriptionName: "regex-sub",
	}
	consumer, err := c.Subscribe(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	err = genMessages(p1, 5, func(idx int) string {
		return fmt.Sprintf("my-topic-message-%d", idx)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = genMessages(p2, 5, func(idx int) string {
		return fmt.Sprintf("foo-message-%d", idx)
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		m, err := consumer.Receive(ctx)
		if err != nil {
			t.Errorf("failed to receive message error: %+v", err)
		} else {
			assert.Truef(t, strings.HasPrefix(string(m.Payload()), "foo-"),
				"message does not start with foo: %s", string(m.Payload()))
		}
	}
}

func runRegexConsumerAddMatchingTopic(t *testing.T, c Client, namespace string) {
	topicInRegex := namespace + "/foo-topic"
	p, err := c.CreateProducer(ProducerOptions{
		Topic:           topicInRegex,
		DisableBatching: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	topicsPattern := fmt.Sprintf("persistent://%s/foo.*", namespace)
	opts := ConsumerOptions{
		TopicsPattern:    topicsPattern,
		SubscriptionName: "regex-sub",
	}
	consumer, err := c.Subscribe(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	consumer.(*regexConsumer).discover()
	time.Sleep(100 * time.Millisecond)

	err = genMessages(p, 5, func(idx int) string {
		return fmt.Sprintf("foo-message-%d", idx)
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		m, err := consumer.Receive(ctx)
		if err != nil {
			t.Errorf("failed to receive message error: %+v", err)
		} else {
			assert.Truef(t, strings.HasPrefix(string(m.Payload()), "foo-"),
				"message does not start with foo: %s", string(m.Payload()))
		}
	}
}

func runRegexConsumerRemoveMatchingTopic(t *testing.T, c Client, namespace string) {
	tn, _ := internal.ParseTopicName(fmt.Sprintf("persistent://%s/.*", namespace))
	pattern := regexp.MustCompile(fmt.Sprintf("%s/.*", namespace))

	opts := ConsumerOptions{
		SubscriptionName:    "regex-sub",
		AutoDiscoveryPeriod: 5 * time.Minute,
	}

	dlq, _ := newDlqRouter(c.(*client), nil, log.DefaultNopLogger())
	rlq, _ := newRetryRouter(c.(*client), nil, false, log.DefaultNopLogger())
	consumer, err := newRegexConsumer(c.(*client), opts, tn, pattern, make(chan ConsumerMessage, 1), dlq, rlq)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	rc := consumer.(*regexConsumer)

	// trigger discovery
	rc.discover()

	consumers := cloneConsumers(rc)
	assert.Equal(t, 0, len(consumers))

	// generate three random topics test they exist and assert three consumers
	tPrefix := generateRandomName()
	topics := make([]string, 3)
	for i := 0; i < 3; i++ {
		topics[i] = fmt.Sprintf("%s/%s-%d", namespace, tPrefix, i)
		if err := createTopic(topics[i]); err != nil {
			t.Fatal(err)
		}
	}

	ptopics, err := getNamespaceTopics(namespace)
	if err != nil {
		t.Fatal(err)
	}
	for _, tp := range topics {
		assert.Contains(t, ptopics, tp)
	}

	rc.discover()
	time.Sleep(2000 * time.Millisecond)

	consumers = cloneConsumers(rc)
	assert.Equal(t, 3, len(consumers))

	// delete 0th random topic
	if err := deleteTopic(topics[0]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1000 * time.Millisecond)
	rc.discover()
	time.Sleep(1000 * time.Millisecond)

	// assert that the topic has not been recreated
	ptopics, err = getNamespaceTopics(namespace)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotContains(t, ptopics, topics[0])
	// assert the other topics are still there
	for _, tp := range topics[1:] {
		assert.Contains(t, ptopics, tp)
	}

	// assert only two consumers now
	consumers = cloneConsumers(rc)
	for _, c := range consumers {
		assert.NotEmpty(t, c.Subscription())
	}
	assert.Equal(t, 2, len(consumers))
}

func genMessages(p Producer, num int, msgFn func(idx int) string) error {
	ctx := context.Background()
	for i := 0; i < num; i++ {
		m := &ProducerMessage{
			Payload: []byte(msgFn(i)),
		}
		if _, err := p.Send(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func cloneConsumers(rc *regexConsumer) map[string]Consumer {
	consumers := make(map[string]Consumer)
	rc.consumersLock.Lock()
	defer rc.consumersLock.Unlock()
	for t, c := range rc.consumers {
		consumers[t] = c
	}
	return consumers
}
