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

// Basic information about a Pulsar connector
type ConnectorDefinition struct {
	// The name of the connector type
	Name string `json:"name"`

	// Description to be used for user help
	Description string `json:"description"`

	// The class name for the connector source implementation
	// <p>If not defined, it will be assumed this connector cannot act as a data source
	SourceClass string `json:"sourceClass"`

	// The class name for the connector sink implementation
	// <p>If not defined, it will be assumed this connector cannot act as a data sink
	SinkClass string `json:"sinkClass"`
}
