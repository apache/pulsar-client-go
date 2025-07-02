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
	switch str {
	case "AutoUpdateDisabled":
		return AutoUpdateDisabled, nil
	case "Backward":
		return Backward, nil
	case "Forward":
		return Forward, nil
	case "Full":
		return Full, nil
	case "AlwaysCompatible":
		return AlwaysCompatible, nil
	case "BackwardTransitive":
		return BackwardTransitive, nil
	case "ForwardTransitive":
		return ForwardTransitive, nil
	case "FullTransitive":
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
	SchemaCompatibilityStrategyUndefined          SchemaCompatibilityStrategy = "UNDEFINED"
	SchemaCompatibilityStrategyAlwaysIncompatible SchemaCompatibilityStrategy = "ALWAYS_INCOMPATIBLE"
	SchemaCompatibilityStrategyAlwaysCompatible   SchemaCompatibilityStrategy = "ALWAYS_COMPATIBLE"
	SchemaCompatibilityStrategyBackward           SchemaCompatibilityStrategy = "BACKWARD"
	SchemaCompatibilityStrategyForward            SchemaCompatibilityStrategy = "FORWARD"
	SchemaCompatibilityStrategyFull               SchemaCompatibilityStrategy = "FULL"
	SchemaCompatibilityStrategyBackwardTransitive SchemaCompatibilityStrategy = "BACKWARD_TRANSITIVE"
	SchemaCompatibilityStrategyForwardTransitive  SchemaCompatibilityStrategy = "FORWARD_TRANSITIVE"
	SchemaCompatibilityStrategyFullTransitive     SchemaCompatibilityStrategy = "FULL_TRANSITIVE"
)

func ParseSchemaCompatibilityStrategy(str string) (SchemaCompatibilityStrategy, error) {
	switch str {
	case "UNDEFINED":
		return SchemaCompatibilityStrategyUndefined, nil
	case "ALWAYS_INCOMPATIBLE":
		return SchemaCompatibilityStrategyAlwaysIncompatible, nil
	case "ALWAYS_COMPATIBLE":
		return SchemaCompatibilityStrategyAlwaysCompatible, nil
	case "BACKWARD":
		return SchemaCompatibilityStrategyBackward, nil
	case "FORWARD":
		return SchemaCompatibilityStrategyForward, nil
	case "FULL":
		return SchemaCompatibilityStrategyFull, nil
	case "BACKWARD_TRANSITIVE":
		return SchemaCompatibilityStrategyBackwardTransitive, nil
	case "FORWARD_TRANSITIVE":
		return SchemaCompatibilityStrategyForwardTransitive, nil
	case "FULL_TRANSITIVE":
		return SchemaCompatibilityStrategyFullTransitive, nil
	default:
		return "", errors.Errorf("Invalid schema compatibility strategy %s", str)
	}
}

func (s SchemaCompatibilityStrategy) String() string {
	return string(s)
}
