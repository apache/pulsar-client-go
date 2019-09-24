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

import "github.com/pkg/errors"

type TopicDomain string

const (
	persistent     TopicDomain = "persistent"
	non_persistent TopicDomain = "non-persistent"
)

func ParseTopicDomain(domain string) (TopicDomain, error) {
	switch domain {
	case "persistent":
		return persistent, nil
	case "non-persistent":
		return non_persistent, nil
	default:
		return "", errors.Errorf("The domain only can be specified as 'persistent' or " +
			"'non-persistent'. Input domain is '%s'.", domain)
	}
}

func (t TopicDomain) String() string {
	return string(t)
}
