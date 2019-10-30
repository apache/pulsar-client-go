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

type SourceConfig struct {
	Tenant    string `json:"tenant" yaml:"tenant"`
	Namespace string `json:"namespace" yaml:"namespace"`
	Name      string `json:"name" yaml:"name"`
	ClassName string `json:"className" yaml:"className"`

	TopicName      string `json:"topicName" yaml:"topicName"`
	SerdeClassName string `json:"serdeClassName" yaml:"serdeClassName"`
	SchemaType     string `json:"schemaType" yaml:"schemaType"`

	Configs map[string]interface{} `json:"configs" yaml:"configs"`

	// This is a map of secretName(aka how the secret is going to be
	// accessed in the function via context) to an object that
	// encapsulates how the secret is fetched by the underlying
	// secrets provider. The type of an value here can be found by the
	// SecretProviderConfigurator.getSecretObjectType() method.
	Secrets map[string]interface{} `json:"secrets" yaml:"secrets"`

	Parallelism          int        `json:"parallelism" yaml:"parallelism"`
	ProcessingGuarantees string     `json:"processingGuarantees" yaml:"processingGuarantees"`
	Resources            *Resources `json:"resources" yaml:"resources"`
	Archive              string     `json:"archive" yaml:"archive"`
	// Any flags that you want to pass to the runtime.
	RuntimeFlags string `json:"runtimeFlags" yaml:"runtimeFlags"`
}
