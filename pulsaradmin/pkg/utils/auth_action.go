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

type AuthAction string

const (
	produce       AuthAction = "produce"
	consume       AuthAction = "consume"
	functionsAuth AuthAction = "functions"
	packages      AuthAction = "packages"
	sinks         AuthAction = "sinks"
	sources       AuthAction = "sources"
)

func ParseAuthAction(action string) (AuthAction, error) {
	switch action {
	case "produce":
		return produce, nil
	case "consume":
		return consume, nil
	case "functions":
		return functionsAuth, nil
	case "packages":
		return packages, nil
	case "sinks":
		return sinks, nil
	case "sources":
		return sources, nil
	default:
		return "", errors.Errorf("The auth action only can be specified as 'produce', "+
			"'consume', 'sources', 'sinks', 'packages', or 'functions'. Invalid auth action '%s'", action)
	}
}

func (a AuthAction) String() string {
	return string(a)
}
