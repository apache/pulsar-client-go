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

package pulsaradmin

import "github.com/pkg/errors"

type AuthAction string

const (
	authProduce   AuthAction = "produce"
	authConsume   AuthAction = "consume"
	authFunctions AuthAction = "functions"
	authPackages  AuthAction = "packages"
	authSinks     AuthAction = "sinks"
	authSources   AuthAction = "sources"
)

func ParseAuthAction(action string) (AuthAction, error) {
	switch action {
	case "produce":
		return authProduce, nil
	case "consume":
		return authConsume, nil
	case "functions":
		return authFunctions, nil
	case "packages":
		return authPackages, nil
	case "sinks":
		return authSinks, nil
	case "sources":
		return authSources, nil
	default:
		return "", errors.Errorf("The auth action only can be specified as 'produce', "+
			"'consume', 'sources', 'sinks', 'packages', or 'functions'. Invalid auth action '%s'", action)
	}
}

func (a AuthAction) String() string {
	return string(a)
}
