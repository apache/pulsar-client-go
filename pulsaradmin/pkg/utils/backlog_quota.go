// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	LimitTime int64           `json:"limitTime"`
	LimitSize int64           `json:"limitSize"`
	Policy    RetentionPolicy `json:"policy"`
}

func NewBacklogQuota(limitSize int64, limitTime int64, policy RetentionPolicy) BacklogQuota {
	return BacklogQuota{
		LimitSize: limitSize,
		LimitTime: limitTime,
		Policy:    policy,
	}
}

type RetentionPolicy string

const (
	ProducerRequestHold     RetentionPolicy = "producer_request_hold"
	ProducerException       RetentionPolicy = "producer_exception"
	ConsumerBacklogEviction RetentionPolicy = "consumer_backlog_eviction"
)

func ParseRetentionPolicy(str string) (RetentionPolicy, error) {
	switch str {
	case ProducerRequestHold.String():
		return ProducerRequestHold, nil
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

type BacklogQuotaType string

const (
	DestinationStorage BacklogQuotaType = "destination_storage"
	MessageAge         BacklogQuotaType = "message_age"
)

func ParseBacklogQuotaType(str string) (BacklogQuotaType, error) {
	switch str {
	case "":
		fallthrough
	case DestinationStorage.String():
		return DestinationStorage, nil
	case MessageAge.String():
		return MessageAge, nil
	default:
		return "", errors.Errorf("Invalid backlog quota type: %s", str)
	}
}

func (b BacklogQuotaType) String() string {
	return string(b)
}
