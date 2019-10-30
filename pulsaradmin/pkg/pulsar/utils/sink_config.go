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
	TopicsPattern *string    `json:"topicsPattern" yaml:"topicsPattern"`
	Resources     *Resources `json:"resources" yaml:"resources"`
	TimeoutMs     *int64     `json:"timeoutMs" yaml:"timeoutMs"`

	// Whether the subscriptions the functions created/used should be deleted when the functions is deleted
	CleanupSubscription bool `json:"cleanupSubscription" yaml:"cleanupSubscription"`

	RetainOrdering bool   `json:"retainOrdering" yaml:"retainOrdering"`
	AutoAck        bool   `json:"autoAck" yaml:"autoAck"`
	Parallelism    int    `json:"parallelism" yaml:"parallelism"`
	Tenant         string `json:"tenant" yaml:"tenant"`
	Namespace      string `json:"namespace" yaml:"namespace"`
	Name           string `json:"name" yaml:"name"`
	ClassName      string `json:"className" yaml:"className"`

	Archive                string                    `json:"archive" yaml:"archive"`
	ProcessingGuarantees   string                    `json:"processingGuarantees" yaml:"processingGuarantees"`
	SourceSubscriptionName string                    `json:"sourceSubscriptionName" yaml:"sourceSubscriptionName"`
	RuntimeFlags           string                    `json:"runtimeFlags" yaml:"runtimeFlags"`
	Inputs                 []string                  `json:"inputs" yaml:"inputs"`
	TopicToSerdeClassName  map[string]string         `json:"topicToSerdeClassName" yaml:"topicToSerdeClassName"`
	TopicToSchemaType      map[string]string         `json:"topicToSchemaType" yaml:"topicToSchemaType"`
	InputSpecs             map[string]ConsumerConfig `json:"inputSpecs" yaml:"inputSpecs"`
	Configs                map[string]interface{}    `json:"configs" yaml:"configs"`

	// This is a map of secretName(aka how the secret is going to be
	// accessed in the function via context) to an object that
	// encapsulates how the secret is fetched by the underlying
	// secrets provider. The type of an value here can be found by the
	// SecretProviderConfigurator.getSecretObjectType() method.
	Secrets map[string]interface{} `json:"secrets" yaml:"secrets"`
}
