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
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
)

type Subscriptions interface {
	Create(TopicName, string, MessageID) error
	Delete(TopicName, string) error
	List(TopicName) ([]string, error)
	ResetCursorToMessageID(TopicName, string, MessageID) error
	ResetCursorToTimestamp(TopicName, string, int64) error
	ClearBacklog(TopicName, string) error
	SkipMessages(TopicName, string, int64) error
	ExpireMessages(TopicName, string, int64) error
	ExpireAllMessages(TopicName, int64) error
	PeekMessages(TopicName, string, int) ([]*Message, error)
}

type subscriptions struct {
	client   *client
	basePath string
	SubPath  string
}

func (c *client) Subscriptions() Subscriptions {
	return &subscriptions{
		client:   c,
		basePath: "",
		SubPath:  "subscription",
	}
}

func (s *subscriptions) Create(topic TopicName, sName string, messageID MessageID) error {
	endpoint := s.client.endpoint(s.basePath, topic.GetRestPath(), s.SubPath, url.QueryEscape(sName))
	return s.client.put(endpoint, messageID)
}

func (s *subscriptions) Delete(topic TopicName, sName string) error {
	endpoint := s.client.endpoint(s.basePath, topic.GetRestPath(), s.SubPath, url.QueryEscape(sName))
	return s.client.delete(endpoint)
}

func (s *subscriptions) List(topic TopicName) ([]string, error) {
	endpoint := s.client.endpoint(s.basePath, topic.GetRestPath(), "subscriptions")
	var list []string
	return list, s.client.get(endpoint, &list)
}

func (s *subscriptions) ResetCursorToMessageID(topic TopicName, sName string, id MessageID) error {
	endpoint := s.client.endpoint(s.basePath, topic.GetRestPath(), s.SubPath, url.QueryEscape(sName), "resetcursor")
	return s.client.post(endpoint, id)
}

func (s *subscriptions) ResetCursorToTimestamp(topic TopicName, sName string, timestamp int64) error {
	endpoint := s.client.endpoint(
		s.basePath, topic.GetRestPath(), s.SubPath, url.QueryEscape(sName),
		"resetcursor", strconv.FormatInt(timestamp, 10))
	return s.client.post(endpoint, "")
}

func (s *subscriptions) ClearBacklog(topic TopicName, sName string) error {
	endpoint := s.client.endpoint(
		s.basePath, topic.GetRestPath(), s.SubPath, url.QueryEscape(sName), "skip_all")
	return s.client.post(endpoint, "")
}

func (s *subscriptions) SkipMessages(topic TopicName, sName string, n int64) error {
	endpoint := s.client.endpoint(
		s.basePath, topic.GetRestPath(), s.SubPath, url.QueryEscape(sName),
		"skip", strconv.FormatInt(n, 10))
	return s.client.post(endpoint, "")
}

func (s *subscriptions) ExpireMessages(topic TopicName, sName string, expire int64) error {
	endpoint := s.client.endpoint(
		s.basePath, topic.GetRestPath(), s.SubPath, url.QueryEscape(sName),
		"expireMessages", strconv.FormatInt(expire, 10))
	return s.client.post(endpoint, "")
}

func (s *subscriptions) ExpireAllMessages(topic TopicName, expire int64) error {
	endpoint := s.client.endpoint(
		s.basePath, topic.GetRestPath(), "all_subscription",
		"expireMessages", strconv.FormatInt(expire, 10))
	return s.client.post(endpoint, "")
}

func (s *subscriptions) PeekMessages(topic TopicName, sName string, n int) ([]*Message, error) {
	var msgs []*Message

	count := 1
	for n > 0 {
		m, err := s.peekNthMessage(topic, sName, count)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m...)
		n -= len(m)
		count++
	}

	return msgs, nil
}

func (s *subscriptions) peekNthMessage(topic TopicName, sName string, pos int) ([]*Message, error) {
	endpoint := s.client.endpoint(s.basePath, topic.GetRestPath(), "subscription", url.QueryEscape(sName),
		"position", strconv.Itoa(pos))
	req, err := s.client.newRequest(http.MethodGet, endpoint)
	if err != nil {
		return nil, err
	}

	resp, err := checkSuccessful(s.client.doRequest(req))
	if err != nil {
		return nil, err
	}
	defer safeRespClose(resp)

	return handleResp(topic, resp)
}

const (
	PublishTimeHeader = "X-Pulsar-Publish-Time"
	BatchHeader       = "X-Pulsar-Num-Batch-Message"
	PropertyPrefix    = "X-Pulsar-PROPERTY-"
)

func handleResp(topic TopicName, resp *http.Response) ([]*Message, error) {
	msgID := resp.Header.Get("X-Pulsar-Message-ID")
	ID, err := ParseMessageID(msgID)
	if err != nil {
		return nil, err
	}

	// read data
	payload, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	properties := make(map[string]string)
	for k := range resp.Header {
		switch {
		case k == PublishTimeHeader:
			h := resp.Header.Get(k)
			if h != "" {
				properties["publish-time"] = h
			}
		case k == BatchHeader:
			h := resp.Header.Get(k)
			if h != "" {
				properties[BatchHeader] = h
			}
			return getIndividualMsgsFromBatch(topic, ID, payload, properties)
		case strings.Contains(k, PropertyPrefix):
			key := strings.TrimPrefix(k, PropertyPrefix)
			properties[key] = resp.Header.Get(k)
		}
	}

	return []*Message{NewMessage(topic.String(), *ID, payload, properties)}, nil
}

func getIndividualMsgsFromBatch(topic TopicName, msgID *MessageID, data []byte,
	properties map[string]string) ([]*Message, error) {

	batchSize, err := strconv.Atoi(properties[BatchHeader])
	if err != nil {
		return nil, nil
	}

	msgs := make([]*Message, 0, batchSize)

	// read all messages in batch
	buf32 := make([]byte, 4)
	rdBuf := bytes.NewReader(data)
	for i := 0; i < batchSize; i++ {
		msgID.BatchIndex = i
		// singleMetaSize
		if _, err := io.ReadFull(rdBuf, buf32); err != nil {
			return nil, err
		}
		singleMetaSize := binary.BigEndian.Uint32(buf32)

		// singleMeta
		singleMetaBuf := make([]byte, singleMetaSize)
		if _, err := io.ReadFull(rdBuf, singleMetaBuf); err != nil {
			return nil, err
		}

		singleMeta := new(SingleMessageMetadata)
		if err := proto.Unmarshal(singleMetaBuf, singleMeta); err != nil {
			return nil, err
		}

		if len(singleMeta.Properties) > 0 {
			for _, v := range singleMeta.Properties {
				k := *v.Key
				property := *v.Value
				properties[k] = property
			}
		}

		//payload
		singlePayload := make([]byte, singleMeta.GetPayloadSize())
		if _, err := io.ReadFull(rdBuf, singlePayload); err != nil {
			return nil, err
		}

		msgs = append(msgs, &Message{
			topic:      topic.String(),
			messageID:  *msgID,
			payload:    singlePayload,
			properties: properties,
		})
	}

	return msgs, nil
}
