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

package utils

// nolint
import "github.com/golang/protobuf/proto"

type Message struct {
	MessageID  MessageID
	Payload    []byte
	Topic      string
	Properties map[string]string
}

func NewMessage(topic string, id MessageID, payload []byte, properties map[string]string) *Message {
	return &Message{
		MessageID:  id,
		Payload:    payload,
		Topic:      topic,
		Properties: properties,
	}
}

func (m *Message) GetMessageID() MessageID {
	return m.MessageID
}

func (m *Message) GetProperties() map[string]string {
	return m.Properties
}

func (m *Message) GetPayload() []byte {
	return m.Payload
}

// nolint
type SingleMessageMetadata struct {
	Properties   []*KeyValue `protobuf:"bytes,1,rep,name=properties" json:"properties,omitempty"`
	PartitionKey *string     `protobuf:"bytes,2,opt,name=partition_key,json=partitionKey" json:"partition_key,omitempty"`
	PayloadSize  *int32      `protobuf:"varint,3,req,name=payload_size,json=payloadSize" json:"payload_size,omitempty"`
	CompactedOut *bool       `protobuf:"varint,4,opt,name=compacted_out,json=compactedOut,def=0" json:"compacted_out,omitempty"`
	// the timestamp that this event occurs. it is typically set by applications.
	// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
	EventTime              *uint64 `protobuf:"varint,5,opt,name=event_time,json=eventTime,def=0" json:"event_time,omitempty"`
	PartitionKeyB64Encoded *bool   `protobuf:"varint,6,opt,name=partition_key_b64_encoded,json=partitionKeyB64Encoded,def=0" json:"partition_key_b64_encoded,omitempty"`
	// Specific a key to overwrite the message key which used for ordering dispatch in Key_Shared mode.
	OrderingKey          []byte   `protobuf:"bytes,7,opt,name=ordering_key,json=orderingKey" json:"ordering_key,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SingleMessageMetadata) Reset()         { *m = SingleMessageMetadata{} }
func (m *SingleMessageMetadata) String() string { return proto.CompactTextString(m) }
func (*SingleMessageMetadata) ProtoMessage()    {}
func (m *SingleMessageMetadata) GetPayloadSize() int32 {
	if m != nil && m.PayloadSize != nil {
		return *m.PayloadSize
	}
	return 0
}

// nolint
type KeyValue struct {
	Key                  *string  `protobuf:"bytes,1,req,name=key" json:"key,omitempty"`
	Value                *string  `protobuf:"bytes,2,req,name=value" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyValue) Reset()         { *m = KeyValue{} }
func (m *KeyValue) String() string { return proto.CompactTextString(m) }
func (*KeyValue) ProtoMessage()    {}
