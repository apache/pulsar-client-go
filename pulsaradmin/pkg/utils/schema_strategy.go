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

import (
	"strings"

	"github.com/pkg/errors"
)

type SchemaAutoUpdateCompatibilityStrategy string

const (
	AutoUpdateDisabled SchemaAutoUpdateCompatibilityStrategy = "AutoUpdateDisabled"
	Backward           SchemaAutoUpdateCompatibilityStrategy = "Backward"
	Forward            SchemaAutoUpdateCompatibilityStrategy = "Forward"
	Full               SchemaAutoUpdateCompatibilityStrategy = "Full"
	AlwaysCompatible   SchemaAutoUpdateCompatibilityStrategy = "AlwaysCompatible"
	BackwardTransitive SchemaAutoUpdateCompatibilityStrategy = "BackwardTransitive"
	ForwardTransitive  SchemaAutoUpdateCompatibilityStrategy = "ForwardTransitive"
	FullTransitive     SchemaAutoUpdateCompatibilityStrategy = "FullTransitive"
)

func ParseSchemaAutoUpdateCompatibilityStrategy(str string) (SchemaAutoUpdateCompatibilityStrategy, error) {
	lowerCaseStr := strings.ToLower(str)
	switch lowerCaseStr {
	case "autoupdatedisabled":
		return AutoUpdateDisabled, nil
	case "backward":
		return Backward, nil
	case "forward":
		return Forward, nil
	case "full":
		return Full, nil
	case "alwayscompatible":
		return AlwaysCompatible, nil
	case "backwardtransitive":
		return BackwardTransitive, nil
	case "forwardtransitive":
		return ForwardTransitive, nil
	case "fulltransitive":
		return FullTransitive, nil
	default:
		return "", errors.Errorf("Invalid schema auto update compatibility strategy %s", str)
	}
}

func (s SchemaAutoUpdateCompatibilityStrategy) String() string {
	return string(s)
}

type SchemaCompatibilityStrategy string

const (
	SchemaCompatibilityStrategyUndefined          SchemaCompatibilityStrategy = "Undefined"
	SchemaCompatibilityStrategyAlwaysIncompatible SchemaCompatibilityStrategy = "AlwaysIncompatible"
	SchemaCompatibilityStrategyAlwaysCompatible   SchemaCompatibilityStrategy = "AlwaysCompatible"
	SchemaCompatibilityStrategyBackward           SchemaCompatibilityStrategy = "Backward"
	SchemaCompatibilityStrategyForward            SchemaCompatibilityStrategy = "Forward"
	SchemaCompatibilityStrategyFull               SchemaCompatibilityStrategy = "Full"
	SchemaCompatibilityStrategyBackwardTransitive SchemaCompatibilityStrategy = "BackwardTransitive"
	SchemaCompatibilityStrategyForwardTransitive  SchemaCompatibilityStrategy = "ForwardTransitive"
	SchemaCompatibilityStrategyFullTransitive     SchemaCompatibilityStrategy = "FullTransitive"
)

func ParseSchemaCompatibilityStrategy(str string) (SchemaCompatibilityStrategy, error) {
	lowerCaseStr := strings.ToLower(str)
	switch lowerCaseStr {
	case "undefined":
		return SchemaCompatibilityStrategyUndefined, nil
	case "alwaysincompatible":
		return SchemaCompatibilityStrategyAlwaysIncompatible, nil
	case "alwayscompatible":
		return SchemaCompatibilityStrategyAlwaysCompatible, nil
	case "backward":
		return SchemaCompatibilityStrategyBackward, nil
	case "forward":
		return SchemaCompatibilityStrategyForward, nil
	case "full":
		return SchemaCompatibilityStrategyFull, nil
	case "backwardtransitive":
		return SchemaCompatibilityStrategyBackwardTransitive, nil
	case "forwardtransitive":
		return SchemaCompatibilityStrategyForwardTransitive, nil
	case "fulltransitive":
		return SchemaCompatibilityStrategyFullTransitive, nil
	default:
		return "", errors.Errorf("Invalid schema compatibility strategy %s", str)
	}
}

func (s SchemaCompatibilityStrategy) String() string {
	return string(s)
}
