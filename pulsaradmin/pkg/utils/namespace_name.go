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
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

type NameSpaceName struct {
	tenant    string
	nameSpace string
}

func GetNameSpaceName(tenant, namespace string) (*NameSpaceName, error) {
	return GetNamespaceName(fmt.Sprintf("%s/%s", tenant, namespace))
}

func GetNamespaceName(completeName string) (*NameSpaceName, error) {
	var n NameSpaceName

	if completeName == "" {
		return nil, errors.New("the namespace complete name is empty")
	}

	parts := strings.Split(completeName, "/")
	if len(parts) == 2 {
		n.tenant = parts[0]
		n.nameSpace = parts[1]
		err := validateNamespaceName(n.tenant, n.nameSpace)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.Errorf("The complete name of namespace is invalid. complete name : [%s]", completeName)
	}

	return &n, nil
}

func (n *NameSpaceName) String() string {
	return fmt.Sprintf("%s/%s", n.tenant, n.nameSpace)
}

func validateNamespaceName(tenant, namespace string) error {
	if tenant == "" || namespace == "" {
		return errors.Errorf("Invalid tenant or namespace. [%s/%s]", tenant, namespace)
	}

	ok := CheckName(tenant)
	if !ok {
		return errors.Errorf("Tenant name include unsupported special chars. tenant : [%s]", tenant)
	}

	ok = CheckName(namespace)
	if !ok {
		return errors.Errorf("Namespace name include unsupported special chars. namespace : [%s]", namespace)
	}

	return nil
}

// allowed characters for property, namespace, cluster and topic
// names are alphanumeric (a-zA-Z0-9) and these special chars -=:.
// and % is allowed as part of valid URL encoding
var patten = regexp.MustCompile(`^[-=:.\w]*$`)

func CheckName(name string) bool {
	return patten.MatchString(name)
}
