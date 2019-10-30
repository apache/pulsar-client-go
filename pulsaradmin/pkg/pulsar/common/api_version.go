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

package common

type APIVersion int

const (
	V1 APIVersion = iota
	V2
	V3
)

const DefaultAPIVersion = "v2"

func (v APIVersion) String() string {
	switch v {
	case V1:
		return ""
	case V2:
		return "v2"
	case V3:
		return "v3"
	}

	return DefaultAPIVersion
}
