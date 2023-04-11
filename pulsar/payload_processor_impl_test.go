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
	"encoding/binary"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProducerConsumerWithDefaultProcessor(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "my-topic1"
	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		Type:                    Exclusive,
		MessagePayloadProcessor: DefaultPayloadProcessor{},
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
		BatchingMaxSize: 10,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
		}, func(mi MessageID, pm *ProducerMessage, err error) {
			assert.Nil(t, err)
		})
	}

	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)

		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		// ack message
		consumer.Ack(msg)
	}
}

type CustomBatchFormatMeta struct {
	numMessages int
}

/**
 * A batch message whose format is customized.
 *
 * 1. First 2 bytes represent the number of messages.
 * 2. Each message is a string, whose format is
 *   1. First 2 bytes represent the length `N`.
 *   2. Followed N bytes are the bytes of the string.
 */
type CustomBatchFormat struct {
}

func (f CustomBatchFormat) Serialize(messages []string) []byte {
	buf := make([]byte, 2, 1024)
	binary.LittleEndian.PutUint16(buf, uint16(len(messages)))
	fmt.Println("len ", len(messages))

	for _, message := range messages {
		sizeBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(sizeBytes, uint16(len(message)))
		buf = append(buf, sizeBytes...)
		buf = append(buf, []byte(message)...)
	}
	return buf
}

func (f CustomBatchFormat) ReadMetaData(data *[]byte) CustomBatchFormatMeta {
	numMsgs := binary.LittleEndian.Uint16(*data)
	*data = (*data)[2:]
	return CustomBatchFormatMeta{
		numMessages: int(numMsgs),
	}
}

func (f CustomBatchFormat) ReadMessage(data *[]byte) []byte {
	size := binary.LittleEndian.Uint16(*data)
	*data = (*data)[2:]
	fmt.Println("Message Size = ", size)
	fmt.Println("Bytes after = ", *data)
	msg := (*data)[:size]
	*data = (*data)[size:]
	return msg
}

type CustomBatchPayloadProcessor struct {
}

func (p CustomBatchPayloadProcessor) Process(
	ctx *MessagePayloadContext,
	payload []byte,
	schema Schema,
	consumer ConsumeMessages) error {
	entryFormat, found := ctx.GetProperty("entry.format")
	if found && entryFormat != "custom" {
		return DefaultPayloadProcessor{}.Process(ctx, payload, schema, consumer)
	}
	fmt.Println("Payload  = ", payload)
	customEntryFormat := CustomBatchFormat{}
	meta := customEntryFormat.ReadMetaData(&payload)

	messages := make([]Message, meta.numMessages)
	fmt.Println("Num Messages = ", meta.numMessages)
	for index := 0; index < meta.numMessages; index++ {
		msgPayload := customEntryFormat.ReadMessage(&payload)
		fmt.Println("message = ", msgPayload)
		fmt.Println("Remaining bytes = ", payload)
		msg := &MessageImpl{
			payLoad:      msgPayload,
			topic:        ctx.topic,
			producerName: *ctx.msgMetaData.ProducerName,
			msgID: &messageID{
				ledgerID:     ctx.msgID.LedgerID(),
				entryID:      ctx.msgID.EntryID(),
				batchSize:    int32(meta.numMessages),
				partitionIdx: ctx.msgID.PartitionIdx(),
				batchIdx:     int32(index),
			},
		}

		messages = append(messages, msg)
	}
	consumer(messages)
	return nil
}

type CustomBatchProducer struct {
	producer Producer
	messages []string
}

// Send method won't send messages immediately, We'll serialize and send only when
// Flush is called.
func (cp *CustomBatchProducer) SendAsync(message string) {
	if cp.messages == nil {
		cp.messages = make([]string, 0, 10)
	}
	cp.messages = append(cp.messages, message)
}

func (cp *CustomBatchProducer) Flush() error {
	customFormat := CustomBatchFormat{}
	payload := customFormat.Serialize(cp.messages)
	cp.messages = nil
	msg := &ProducerMessage{
		Payload: payload,
		Properties: map[string]string{
			"entry.format": "custom",
		},
	}
	_, err := cp.producer.Send(context.Background(), msg)
	if err != nil {
		return err
	}
	return nil
}

func (cp *CustomBatchProducer) Close() {
	cp.producer.Close()
}

func TestProducerConsumerWithCustomProcessor(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "my-topic2"
	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		Type:                    Exclusive,
		MessagePayloadProcessor: CustomBatchPayloadProcessor{},
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	customBatchMsgProducer := CustomBatchProducer{producer: producer}

	// send 10 messages
	for i := 0; i < 10; i++ {
		customBatchMsgProducer.SendAsync(fmt.Sprintf("hello-%d", i))
	}
	customBatchMsgProducer.Flush()

	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		// ack message
		consumer.Ack(msg)
	}
}
