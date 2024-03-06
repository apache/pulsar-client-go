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
	Tenant    string `json:"tenant,omitempty" yaml:"tenant"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace"`
	Name      string `json:"name,omitempty" yaml:"name"`
	ClassName string `json:"className,omitempty" yaml:"className"`

	ProducerConfig *ProducerConfig `json:"producerConfig,omitempty" yaml:"producerConfig"`

	TopicName      string `json:"topicName,omitempty" yaml:"topicName"`
	SerdeClassName string `json:"serdeClassName,omitempty" yaml:"serdeClassName"`
	SchemaType     string `json:"schemaType,omitempty" yaml:"schemaType"`

	Configs map[string]interface{} `json:"configs,omitempty" yaml:"configs"`

	// This is a map of secretName(aka how the secret is going to be
	// accessed in the function via context) to an object that
	// encapsulates how the secret is fetched by the underlying
	// secrets provider. The type of an value here can be found by the
	// SecretProviderConfigurator.getSecretObjectType() method.
	Secrets map[string]interface{} `json:"secrets,omitempty" yaml:"secrets"`

	Parallelism          int        `json:"parallelism,omitempty" yaml:"parallelism"`
	ProcessingGuarantees string     `json:"processingGuarantees,omitempty" yaml:"processingGuarantees"`
	Resources            *Resources `json:"resources,omitempty" yaml:"resources"`
	Archive              string     `json:"archive,omitempty" yaml:"archive"`
	// Any flags that you want to pass to the runtime.
	RuntimeFlags string `json:"runtimeFlags,omitempty" yaml:"runtimeFlags"`

	CustomRuntimeOptions string `json:"customRuntimeOptions,omitempty" yaml:"customRuntimeOptions"`

	BatchSourceConfig *BatchSourceConfig `json:"batchSourceConfig,omitempty" yaml:"batchSourceConfig"`
	BatchBuilder      string             `json:"batchBuilder,omitempty" yaml:"batchBuilder"`
}
