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

type  ProcessingGuarantees int

type Runtime int

const (
    AtLeasetOnce ProcessingGuarantees = iota
    AtMostOnce
    EffectivelyOnce
)

const (
    Java Runtime = iota
    Python
    Go
)

type FunctionConfig struct {
    // Any flags that you want to pass to the runtime.
    // note that in thread mode, these flags will have no impact
    RuntimeFlags string `json:"runtimeFlags" yaml:"runtimeFlags"`

    Tenant    string `json:"tenant" yaml:"tenant"`
    Namespace string `json:"namespace" yaml:"namespace"`
    Name      string `json:"name" yaml:"name"`
    ClassName string `json:"className" yaml:"className"`

    Inputs             []string          `json:"inputs" yaml:"inputs"`
    CustomSerdeInputs  map[string]string `json:"customSerdeInputs" yaml:"customSerdeInputs"`
    TopicsPattern      *string           `json:"topicsPattern" yaml:"topicsPattern"`
    CustomSchemaInputs map[string]string `json:"customSchemaInputs" yaml:"customSchemaInputs"`

    // A generalized way of specifying inputs
    InputSpecs map[string]ConsumerConfig `json:"inputSpecs" yaml:"inputSpecs"`

    Output string `json:"output" yaml:"output"`

    // Represents either a builtin schema type (eg: 'avro', 'json', ect) or the class name for a Schema implementation
    OutputSchemaType string `json:"outputSchemaType" yaml:"outputSchemaType"`

    OutputSerdeClassName string                 `json:"outputSerdeClassName" yaml:"outputSerdeClassName"`
    LogTopic             string                 `json:"logTopic" yaml:"logTopic"`
    ProcessingGuarantees ProcessingGuarantees   `json:"processingGuarantees" yaml:"processingGuarantees"`
    RetainOrdering       bool                   `json:"retainOrdering" yaml:"retainOrdering"`
    UserConfig           map[string]interface{} `json:"userConfig" yaml:"userConfig"`

    // This is a map of secretName(aka how the secret is going to be
    // accessed in the function via context) to an object that
    // encapsulates how the secret is fetched by the underlying
    // secrets provider. The type of an value here can be found by the
    // SecretProviderConfigurator.getSecretObjectType() method.
    Secrets map[string]interface{} `json:"secrets" yaml:"secrets"`

    Runtime           Runtime       `json:"runtime" yaml:"runtime"`
    AutoAck           bool          `json:"autoAck" yaml:"autoAck"`
    MaxMessageRetries int           `json:"maxMessageRetries" yaml:"maxMessageRetries"`
    DeadLetterTopic   string        `json:"deadLetterTopic" yaml:"deadLetterTopic"`
    SubName           string        `json:"subName" yaml:"subName"`
    Parallelism       int           `json:"parallelism" yaml:"parallelism"`
    Resources         *Resources    `json:"resources" yaml:"resources"`
    FQFN              string        `json:"fqfn" yaml:"fqfn"`
    WindowConfig      *WindowConfig `json:"windowConfig" yaml:"windowConfig"`
    TimeoutMs         *int64         `json:"timeoutMs" yaml:"timeoutMs"`
    Jar               string        `json:"jar" yaml:"jar"`
    Py                string        `json:"py" yaml:"py"`
    Go                string        `json:"go" yaml:"go"`
    // Whether the subscriptions the functions created/used should be deleted when the functions is deleted
    CleanupSubscription bool `json:"cleanupSubscription" yaml:"cleanupSubscription"`
}
