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

type SinkConfig struct {
    Tenant    string
    Namespace string
    Name      string
    ClassName string

    SourceSubscriptionName string
    Inputs                 []string
    TopicToSerdeClassName  map[string]string
    TopicsPattern          string
    TopicToSchemaType      map[string]string
    InputSpecs             map[string]ConsumerConfig
    Configs                map[string]interface{}

    // This is a map of secretName(aka how the secret is going to be
    // accessed in the function via context) to an object that
    // encapsulates how the secret is fetched by the underlying
    // secrets provider. The type of an value here can be found by the
    // SecretProviderConfigurator.getSecretObjectType() method.
    Secrets              map[string]interface{}
    Parallelism          int
    ProcessingGuarantees ProcessingGuarantees
    RetainOrdering       bool
    Resources            Resources
    AutoAck              bool
    TimeoutMs            int64
    Archive              string

    // Whether the subscriptions the functions created/used should be deleted when the functions is deleted
    CleanupSubscription bool
    RuntimeFlags        string
}
