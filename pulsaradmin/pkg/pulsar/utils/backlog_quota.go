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

type BacklogQuota struct {
	Limit  int64           `json:"limit"`
	Policy RetentionPolicy `json:"policy"`
}

func NewBacklogQuota(limit int64, policy RetentionPolicy) BacklogQuota {
	return BacklogQuota{
		Limit:  limit,
		Policy: policy,
	}
}

type RetentionPolicy string

type BacklogQuotaType string

const DestinationStorage BacklogQuotaType = "destination_storage"

const (
	ProducerRequestHold     RetentionPolicy = "producer_request_hold"
	ProducerException       RetentionPolicy = "producer_exception"
	ConsumerBacklogEviction RetentionPolicy = "consumer_backlog_eviction"
)

func ParseRetentionPolicy(str string) (RetentionPolicy, error) {
	switch str {
	case ProducerException.String():
		return ProducerException, nil
	case ConsumerBacklogEviction.String():
		return ConsumerBacklogEviction, nil
	default:
		return "", errors.Errorf("Invalid retention policy %s", str)
	}
}

func (s RetentionPolicy) String() string {
	return string(s)
}
