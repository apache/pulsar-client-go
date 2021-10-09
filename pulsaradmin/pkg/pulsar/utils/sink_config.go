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

type SinkConfig struct {
	TopicsPattern *string    `json:"topicsPattern,omitempty" yaml:"topicsPattern"`
	Resources     *Resources `json:"resources,omitempty" yaml:"resources"`
	TimeoutMs     *int64     `json:"timeoutMs,omitempty" yaml:"timeoutMs"`

	// Whether the subscriptions the functions created/used should be deleted when the functions is deleted
	CleanupSubscription bool `json:"cleanupSubscription,omitempty" yaml:"cleanupSubscription"`

	RetainOrdering bool   `json:"retainOrdering,omitempty" yaml:"retainOrdering"`
	AutoAck        bool   `json:"autoAck,omitempty" yaml:"autoAck"`
	Parallelism    int    `json:"parallelism,omitempty" yaml:"parallelism"`
	Tenant         string `json:"tenant,omitempty" yaml:"tenant"`
	Namespace      string `json:"namespace,omitempty" yaml:"namespace"`
	Name           string `json:"name,omitempty" yaml:"name"`
	ClassName      string `json:"className,omitempty" yaml:"className"`

	Archive                    string `json:"archive,omitempty" yaml:"archive"`
	ProcessingGuarantees       string `json:"processingGuarantees,omitempty" yaml:"processingGuarantees"`
	SourceSubscriptionName     string `json:"sourceSubscriptionName,omitempty" yaml:"sourceSubscriptionName"`
	SourceSubscriptionPosition string `json:"sourceSubscriptionPosition,omitempty" yaml:"sourceSubscriptionPosition"`
	RuntimeFlags               string `json:"runtimeFlags,omitempty" yaml:"runtimeFlags"`

	Inputs                []string                  `json:"inputs,omitempty" yaml:"inputs"`
	TopicToSerdeClassName map[string]string         `json:"topicToSerdeClassName,omitempty" yaml:"topicToSerdeClassName"`
	TopicToSchemaType     map[string]string         `json:"topicToSchemaType,omitempty" yaml:"topicToSchemaType"`
	InputSpecs            map[string]ConsumerConfig `json:"inputSpecs,omitempty" yaml:"inputSpecs"`
	Configs               map[string]interface{}    `json:"configs,omitempty" yaml:"configs"`

	CustomRuntimeOptions string `json:"customRuntimeOptions,omitempty" yaml:"customRuntimeOptions"`

	// This is a map of secretName(aka how the secret is going to be
	// accessed in the function via context) to an object that
	// encapsulates how the secret is fetched by the underlying
	// secrets provider. The type of an value here can be found by the
	// SecretProviderConfigurator.getSecretObjectType() method.
	Secrets map[string]interface{} `json:"secrets,omitempty" yaml:"secrets"`
}
