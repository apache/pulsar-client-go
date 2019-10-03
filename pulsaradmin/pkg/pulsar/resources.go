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

package pulsar

type Resources struct {
	CPU  float64 `json:"cpu"`
	Disk int64   `json:"disk"`
	RAM  int64   `json:"ram"`
}

func NewDefaultResources() *Resources {
	resources := &Resources{
		//Default cpu is 1 core
		CPU: 1,
		// Default memory is 1GB
		Disk: 1073741824,
		// Default disk is 10GB
		RAM: 10737418240,
	}

	return resources
}
