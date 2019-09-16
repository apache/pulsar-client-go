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

// ClusterData information on a cluster
type ClusterData struct {
	Name                string   `json:"-"`
	ServiceURL          string   `json:"serviceUrl"`
	ServiceURLTls       string   `json:"serviceUrlTls"`
	BrokerServiceURL    string   `json:"brokerServiceUrl"`
	BrokerServiceURLTls string   `json:"brokerServiceUrlTls"`
	PeerClusterNames    []string `json:"peerClusterNames"`
}

// FunctionData information for a Pulsar Function
type FunctionData struct {
	FQFN                      string  `json:"fqfn"`
	Tenant                    string  `json:"tenant"`
	Namespace                 string  `json:"namespace"`
	FuncName                  string  `json:"functionName"`
	InstanceID                string  `json:"instance_id"`
	ClassName                 string  `json:"className"`
	Jar                       string  `json:"jarFile"`
	Py                        string  `json:"pyFile"`
	Go                        string  `json:"goFile"`
	Inputs                    string  `json:"inputs"`
	TopicsPattern             string  `json:"topicsPattern"`
	Output                    string  `json:"output"`
	LogTopic                  string  `json:"logTopic"`
	SchemaType                string  `json:"schemaType"`
	CustomSerDeInputs         string  `json:"customSerdeInputString"`
	CustomSchemaInput         string  `json:"customSchemaInputString"`
	OutputSerDeClassName      string  `json:"outputSerdeClassName"`
	FunctionConfigFile        string  `json:"fnConfigFile"`
	ProcessingGuarantees      string  `json:"processingGuarantees"`
	UserConfig                string  `json:"userConfigString"`
	RetainOrdering            bool    `json:"retainOrdering"`
	SubsName                  string  `json:"subsName"`
	Parallelism               int     `json:"parallelism"`
	CPU                       float64 `json:"cpu"`
	RAM                       int64   `json:"ram"`
	Disk                      int64   `json:"disk"`
	WindowLengthCount         int     `json:"windowLengthCount"`
	WindowLengthDurationMs    int64   `json:"windowLengthDurationMs"`
	SlidingIntervalCount      int     `json:"slidingIntervalCount"`
	SlidingIntervalDurationMs int64   `json:"slidingIntervalDurationMs"`
	AutoAck                   bool    `json:"autoAck"`
	TimeoutMs                 int64   `json:"timeoutMs"`
	MaxMessageRetries         int     `json:"maxMessageRetries"`
	DeadLetterTopic           string  `json:"deadLetterTopic"`

	Key          string `json:"key"`
	Watch        bool   `json:"watch"`
	State        string `json:"state"`
	TriggerValue string `json:"triggerValue"`
	TriggerFile  string `json:"triggerFile"`
	Topic        string `json:"topic"`

	UpdateAuthData bool `json:"updateAuthData"`

	FuncConf     *FunctionConfig `json:"-"`
	UserCodeFile string          `json:"-"`
}

// Failure Domain information
type FailureDomainData struct {
	ClusterName string   `json:"-"`
	DomainName  string   `json:"-"`
	BrokerList  []string `json:"brokers"`
}

type FailureDomainMap map[string]FailureDomainData

// Tenant args
type TenantData struct {
	Name            string   `json:"-"`
	AdminRoles      []string `json:"adminRoles"`
	AllowedClusters []string `json:"allowedClusters"`
}

type SourceData struct {
	Tenant                   string  `json:"tenant"`
	Namespace                string  `json:"namespace"`
	Name                     string  `json:"name"`
	SourceType               string  `json:"sourceType"`
	ProcessingGuarantees     string  `json:"processingGuarantees"`
	DestinationTopicName     string  `json:"destinationTopicName"`
	DeserializationClassName string  `json:"deserializationClassName"`
	SchemaType               string  `json:"schemaType"`
	Parallelism              int     `json:"parallelism"`
	Archive                  string  `json:"archive"`
	ClassName                string  `json:"className"`
	SourceConfigFile         string  `json:"sourceConfigFile"`
	CPU                      float64 `json:"cpu"`
	RAM                      int64   `json:"ram"`
	Disk                     int64   `json:"disk"`
	SourceConfigString       string  `json:"sourceConfigString"`

	SourceConf *SourceConfig `json:"-"`
	InstanceID string        `json:"instanceId"`

	UpdateAuthData bool `json:"updateAuthData"`
}

type SinkData struct {
	Tenant                  string  `json:"tenant"`
	Namespace               string  `json:"namespace"`
	Name                    string  `json:"name"`
	SinkType                string  `json:"sinkType"`
	Inputs                  string  `json:"inputs"`
	TopicsPattern           string  `json:"topicsPattern"`
	SubsName                string  `json:"subsName"`
	CustomSerdeInputString  string  `json:"customSerdeInputString"`
	CustomSchemaInputString string  `json:"customSchemaInputString"`
	ProcessingGuarantees    string  `json:"processingGuarantees"`
	RetainOrdering          bool    `json:"retainOrdering"`
	Parallelism             int     `json:"parallelism"`
	Archive                 string  `json:"archive"`
	ClassName               string  `json:"className"`
	SinkConfigFile          string  `json:"sinkConfigFile"`
	CPU                     float64 `json:"cpu"`
	RAM                     int64   `json:"ram"`
	Disk                    int64   `json:"disk"`
	SinkConfigString        string  `json:"sinkConfigString"`
	AutoAck                 bool    `json:"autoAck"`
	TimeoutMs               int64   `json:"timeoutMs"`

	SinkConf   *SinkConfig `json:"-"`
	InstanceID string      `json:"instanceId"`

	UpdateAuthData bool `json:"updateAuthData"`
}

// Topic data
type PartitionedTopicMetadata struct {
	Partitions int `json:"partitions"`
}

type SchemaData struct {
	Version         int64  `json:"version"`
	Filename        string `json:"filename"`
	Jar             string `json:"jar"`
	Type            string `json:"type"`
	Classname       string `json:"classname"`
	AlwaysAllowNull bool   `json:"alwaysAllowNull"`
	DryRun          bool   `json:"dryRun"`
}

type LookupData struct {
	BrokerUrl string `json:"brokerUrl"`
	BrokerUrlTls string `json:"brokerUrlTls"`
	HttpUrl string `json:"httpUrl"`
	HttpUrlTls string `json:"httpUrlTls"`
}

