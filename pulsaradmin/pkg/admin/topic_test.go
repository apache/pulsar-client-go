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

package admin

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

func TestCreateTopic(t *testing.T) {
	checkError := func(err error) {
		if err != nil {
			t.Error(err)
		}
	}

	cfg := &config.Config{}
	admin, err := New(cfg)
	checkError(err)

	topic := "persistent://public/default/testCreateTopic"

	topicName, err := utils.GetTopicName(topic)
	checkError(err)

	err = admin.Topics().Create(*topicName, 0)
	checkError(err)

	topicLists, err := admin.Namespaces().GetTopics("public/default")
	checkError(err)

	for _, t := range topicLists {
		if t == topic {
			return
		}
	}
	t.Error("Couldn't find topic: " + topic)
}
