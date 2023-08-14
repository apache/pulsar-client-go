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

import (
	"github.com/pkg/errors"
)

type NamespaceIsolationData struct {
	Namespaces         []string               `json:"namespaces"`
	Primary            []string               `json:"primary"`
	Secondary          []string               `json:"secondary"`
	AutoFailoverPolicy AutoFailoverPolicyData `json:"auto_failover_policy"`
}

type AutoFailoverPolicyData struct {
	PolicyType AutoFailoverPolicyType `json:"policy_type"`
	Parameters map[string]string      `json:"parameters"`
}

type AutoFailoverPolicyType string

const (
	MinAvailable AutoFailoverPolicyType = "min_available"
)

func fromString(autoFailoverPolicyTypeName string) AutoFailoverPolicyType {
	switch autoFailoverPolicyTypeName {
	case "min_available":
		return MinAvailable
	default:
		return ""
	}
}

func CreateNamespaceIsolationData(namespaces, primary, secondry []string, autoFailoverPolicyTypeName string,
	autoFailoverPolicyParams map[string]string) (*NamespaceIsolationData, error) {
	nsIsolationData := new(NamespaceIsolationData)
	if len(namespaces) == 0 {
		return nil, errors.New("unable to parse namespaces parameter list")
	}

	if len(primary) == 0 {
		return nil, errors.New("unable to parse primary parameter list")
	}

	if len(secondry) == 0 {
		return nil, errors.New("unable to parse secondry parameter list")
	}

	nsIsolationData.Namespaces = namespaces
	nsIsolationData.Primary = primary
	nsIsolationData.Secondary = secondry
	nsIsolationData.AutoFailoverPolicy.PolicyType = fromString(autoFailoverPolicyTypeName)
	nsIsolationData.AutoFailoverPolicy.Parameters = autoFailoverPolicyParams

	// validation if necessary
	if nsIsolationData.AutoFailoverPolicy.PolicyType == MinAvailable {
		err := true
		expectParamKeys := []string{"min_limit", "usage_threshold"}

		if len(autoFailoverPolicyParams) == len(expectParamKeys) {
			for _, paramKey := range expectParamKeys {
				if _, ok := autoFailoverPolicyParams[paramKey]; !ok {
					break
				}
			}
			err = false
		}

		if err {
			return nil, errors.Errorf("Unknown auto failover policy params specified: %v", autoFailoverPolicyParams)
		}
	} else {
		// either we don't handle the new type or user has specified a bad type
		return nil, errors.Errorf("Unknown auto failover policy type specified : %v", autoFailoverPolicyTypeName)
	}

	return nsIsolationData, nil
}
