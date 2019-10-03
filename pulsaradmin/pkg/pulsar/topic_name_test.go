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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTopicName(t *testing.T) {
	success, err := GetTopicName("success")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public/default/success", success.String())

	success, err = GetTopicName("tenant/namespace/success")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://tenant/namespace/success", success.String())

	success, err = GetTopicName("persistent://tenant/namespace/success")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://tenant/namespace/success", success.String())

	success, err = GetTopicName("non-persistent://tenant/namespace/success")
	assert.Nil(t, err)
	assert.Equal(t, "non-persistent://tenant/namespace/success", success.String())

	_, err = GetTopicName("://tenant.namespace.topic")
	assert.NotNil(t, err)
	assert.Equal(t, "The domain only can be specified as 'persistent' or 'non-persistent'."+
		" Input domain is ''.", err.Error())

	fail, err := GetTopicName("default/fail")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid short topic name 'default/fail', it should be in the "+
		"format of <tenant>/<namespace>/<topic> or <topic>", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("domain://tenant/namespace/fail")
	assert.NotNil(t, err)
	assert.Equal(t, "The domain only can be specified as 'persistent' or 'non-persistent'. "+
		"Input domain is 'domain'.", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("persistent:///tenant/namespace/fail")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid tenant or namespace. [/tenant]", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("persistent://tenant/namespace")
	assert.NotNil(t, err)
	assert.Equal(t, "invalid topic name 'tenant/namespace', it should be in the format "+
		"of <tenant>/<namespace>/<topic>", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("persistent://tenant/namespace/")
	assert.NotNil(t, err)
	assert.Equal(t, "topic name can not be empty", err.Error())
	assert.Nil(t, fail)
}

func TestTopicNameEncodeTest(t *testing.T) {
	encodedName := "a%3Aen-in_in_business_content_item_20150312173022_https%5C%3A%2F%2Fin.news.example.com%2Fr"
	rawName := "a:en-in_in_business_content_item_20150312173022_https\\://in.news.example.com/r"

	assert.Equal(t, encodedName, url.QueryEscape(rawName))
	o, err := url.QueryUnescape(encodedName)
	assert.Nil(t, err)
	assert.Equal(t, rawName, o)

	topicName, err := GetTopicName("persistent://prop/ns/" + rawName)
	assert.Nil(t, err)

	assert.Equal(t, rawName, topicName.topic)
	assert.Equal(t, encodedName, topicName.GetEncodedTopic())
	assert.Equal(t, "persistent/prop/ns/"+encodedName, topicName.GetRestPath())
}
