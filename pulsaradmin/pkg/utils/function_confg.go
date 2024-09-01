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

const (
	JavaRuntime   = "JAVA"
	PythonRuntime = "PYTHON"
	GoRuntime     = "GO"
)

type FunctionConfig struct {
	TimeoutMs     *int64  `json:"timeoutMs,omitempty" yaml:"timeoutMs"`
	TopicsPattern *string `json:"topicsPattern,omitempty" yaml:"topicsPattern"`
	// Whether the subscriptions the functions created/used should be deleted when the functions is deleted
	CleanupSubscription          bool   `json:"cleanupSubscription" yaml:"cleanupSubscription"`
	RetainOrdering               bool   `json:"retainOrdering" yaml:"retainOrdering"`
	RetainKeyOrdering            bool   `json:"retainKeyOrdering" yaml:"retainKeyOrdering"`
	BatchBuilder                 string `json:"batchBuilder,omitempty" yaml:"batchBuilder"`
	ForwardSourceMessageProperty bool   `json:"forwardSourceMessageProperty" yaml:"forwardSourceMessageProperty"`
	AutoAck                      bool   `json:"autoAck" yaml:"autoAck"`
	Parallelism                  int    `json:"parallelism,omitempty" yaml:"parallelism"`
	MaxMessageRetries            *int   `json:"maxMessageRetries,omitempty" yaml:"maxMessageRetries"`

	Output string `json:"output,omitempty" yaml:"output"`

	ProducerConfig      *ProducerConfig   `json:"producerConfig,omitempty" yaml:"producerConfig"`
	CustomSchemaOutputs map[string]string `json:"customSchemaOutputs,omitempty" yaml:"customSchemaOutputs"`

	OutputSerdeClassName string `json:"outputSerdeClassName,omitempty" yaml:"outputSerdeClassName"`
	LogTopic             string `json:"logTopic,omitempty" yaml:"logTopic"`
	ProcessingGuarantees string `json:"processingGuarantees,omitempty" yaml:"processingGuarantees"`

	// Represents either a builtin schema type (eg: 'avro', 'json', etc) or the class name for a Schema implementation
	OutputSchemaType    string `json:"outputSchemaType,omitempty" yaml:"outputSchemaType"`
	OutputTypeClassName string `json:"outputTypeClassName,omitempty" yaml:"outputTypeClassName"`

	Runtime         string  `json:"runtime,omitempty" yaml:"runtime"`
	DeadLetterTopic string  `json:"deadLetterTopic,omitempty" yaml:"deadLetterTopic"`
	SubName         string  `json:"subName,omitempty" yaml:"subName"`
	FQFN            string  `json:"fqfn,omitempty" yaml:"fqfn"`
	Jar             *string `json:"jar,omitempty" yaml:"jar"`
	Py              *string `json:"py,omitempty" yaml:"py"`
	Go              *string `json:"go,omitempty" yaml:"go"`
	FunctionType    *string `json:"functionType,omitempty" yaml:"functionType"`
	// Any flags that you want to pass to the runtime.
	// note that in thread mode, these flags will have no impact
	RuntimeFlags string `json:"runtimeFlags,omitempty" yaml:"runtimeFlags"`

	Tenant    string `json:"tenant,omitempty" yaml:"tenant"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace"`
	Name      string `json:"name,omitempty" yaml:"name"`
	ClassName string `json:"className,omitempty" yaml:"className"`

	Resources          *Resources             `json:"resources,omitempty" yaml:"resources"`
	WindowConfig       *WindowConfig          `json:"windowConfig,omitempty" yaml:"windowConfig"`
	Inputs             []string               `json:"inputs,omitempty" yaml:"inputs"`
	UserConfig         map[string]interface{} `json:"userConfig,omitempty" yaml:"userConfig"`
	CustomSerdeInputs  map[string]string      `json:"customSerdeInputs,omitempty" yaml:"customSerdeInputs"`
	CustomSchemaInputs map[string]string      `json:"customSchemaInputs,omitempty" yaml:"customSchemaInputs"`

	// A generalized way of specifying inputs
	InputSpecs         map[string]ConsumerConfig `json:"inputSpecs,omitempty" yaml:"inputSpecs"`
	InputTypeClassName string                    `json:"inputTypeClassName,omitempty" yaml:"inputTypeClassName"`

	CustomRuntimeOptions string `json:"customRuntimeOptions,omitempty" yaml:"customRuntimeOptions"`

	// This is a map of secretName(aka how the secret is going to be
	// accessed in the function via context) to an object that
	// encapsulates how the secret is fetched by the underlying
	// secrets provider. The type of an value here can be found by the
	// SecretProviderConfigurator.getSecretObjectType() method.
	Secrets map[string]interface{} `json:"secrets,omitempty" yaml:"secrets"`

	MaxPendingAsyncRequests int `json:"maxPendingAsyncRequests,omitempty" yaml:"maxPendingAsyncRequests"`
	//nolint
	ExposePulsarAdminClientEnabled bool   `json:"exposePulsarAdminClientEnabled" yaml:"exposePulsarAdminClientEnabled"`
	SkipToLatest                   bool   `json:"skipToLatest" yaml:"skipToLatest"`
	SubscriptionPosition           string `json:"subscriptionPosition,omitempty" yaml:"subscriptionPosition"`
}
