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

import "github.com/pkg/errors"

type TopicType string

const (
	Partitioned    TopicType = "partitioned"
	NonPartitioned TopicType = "non-partitioned"
)

func ParseTopicType(topicType string) (TopicType, error) {
	switch topicType {
	case "partitioned":
		return Partitioned, nil
	case "non-partitioned":
		return NonPartitioned, nil
	default:
		return "", errors.Errorf("The topic type can only be specified as 'partitioned' or "+
			"'non-partitioned'. Input topic type is '%s'.", topicType)
	}
}

func (t TopicType) String() string {
	return string(t)
}
