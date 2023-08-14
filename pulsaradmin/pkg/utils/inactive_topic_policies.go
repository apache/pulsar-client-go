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

type InactiveTopicDeleteMode string

const (
	// The topic can be deleted when no subscriptions and no active producers.
	DeleteWhenNoSubscriptions InactiveTopicDeleteMode = "delete_when_no_subscriptions"
	// The topic can be deleted when all subscriptions catchup and no active producers/consumers.
	DeleteWhenSubscriptionsCaughtUp InactiveTopicDeleteMode = "delete_when_subscriptions_caught_up"
)

func (i InactiveTopicDeleteMode) String() string {
	return string(i)
}

func ParseInactiveTopicDeleteMode(str string) (InactiveTopicDeleteMode, error) {
	switch str {
	case DeleteWhenNoSubscriptions.String():
		return DeleteWhenNoSubscriptions, nil
	case DeleteWhenSubscriptionsCaughtUp.String():
		return DeleteWhenSubscriptionsCaughtUp, nil
	default:
		return "", errors.Errorf("cannot parse %s to InactiveTopicDeleteMode type", str)
	}
}

type InactiveTopicPolicies struct {
	InactiveTopicDeleteMode    *InactiveTopicDeleteMode `json:"inactiveTopicDeleteMode"`
	MaxInactiveDurationSeconds int                      `json:"maxInactiveDurationSeconds"`
	DeleteWhileInactive        bool                     `json:"deleteWhileInactive"`
}

func NewInactiveTopicPolicies(inactiveTopicDeleteMode *InactiveTopicDeleteMode, maxInactiveDurationSeconds int,
	deleteWhileInactive bool) InactiveTopicPolicies {
	return InactiveTopicPolicies{
		InactiveTopicDeleteMode:    inactiveTopicDeleteMode,
		MaxInactiveDurationSeconds: maxInactiveDurationSeconds,
		DeleteWhileInactive:        deleteWhileInactive,
	}
}
