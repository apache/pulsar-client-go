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
	UpdateAuthData       bool `json:"updateAuthData"`
	RetainOrdering       bool `json:"retainOrdering"`
	Watch                bool `json:"watch"`
	AutoAck              bool `json:"autoAck"`
	Parallelism          int  `json:"parallelism"`
	WindowLengthCount    int  `json:"windowLengthCount"`
	SlidingIntervalCount int  `json:"slidingIntervalCount"`
	MaxMessageRetries    int  `json:"maxMessageRetries"`

	TimeoutMs                 int64   `json:"timeoutMs"`
	SlidingIntervalDurationMs int64   `json:"slidingIntervalDurationMs"`
	WindowLengthDurationMs    int64   `json:"windowLengthDurationMs"`
	RAM                       int64   `json:"ram"`
	Disk                      int64   `json:"disk"`
	CPU                       float64 `json:"cpu"`
	SubsName                  string  `json:"subsName"`
	DeadLetterTopic           string  `json:"deadLetterTopic"`
	Key                       string  `json:"key"`
	State                     string  `json:"state"`
	TriggerValue              string  `json:"triggerValue"`
	TriggerFile               string  `json:"triggerFile"`
	Topic                     string  `json:"topic"`

	UserCodeFile         string          `json:"-"`
	FQFN                 string          `json:"fqfn"`
	Tenant               string          `json:"tenant"`
	Namespace            string          `json:"namespace"`
	FuncName             string          `json:"functionName"`
	InstanceID           string          `json:"instance_id"`
	ClassName            string          `json:"className"`
	Jar                  string          `json:"jarFile"`
	Py                   string          `json:"pyFile"`
	Go                   string          `json:"goFile"`
	Inputs               string          `json:"inputs"`
	TopicsPattern        string          `json:"topicsPattern"`
	Output               string          `json:"output"`
	LogTopic             string          `json:"logTopic"`
	SchemaType           string          `json:"schemaType"`
	CustomSerDeInputs    string          `json:"customSerdeInputString"`
	CustomSchemaInput    string          `json:"customSchemaInputString"`
	OutputSerDeClassName string          `json:"outputSerdeClassName"`
	FunctionConfigFile   string          `json:"fnConfigFile"`
	ProcessingGuarantees string          `json:"processingGuarantees"`
	UserConfig           string          `json:"userConfigString"`
	DestinationFile      string          `json:"destinationFile"`
	Path                 string          `json:"path"`
	FuncConf             *FunctionConfig `json:"-"`
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
	UpdateAuthData          bool        `json:"updateAuthData"`
	RetainOrdering          bool        `json:"retainOrdering"`
	AutoAck                 bool        `json:"autoAck"`
	Parallelism             int         `json:"parallelism"`
	RAM                     int64       `json:"ram"`
	Disk                    int64       `json:"disk"`
	TimeoutMs               int64       `json:"timeoutMs"`
	CPU                     float64     `json:"cpu"`
	Tenant                  string      `json:"tenant"`
	Namespace               string      `json:"namespace"`
	Name                    string      `json:"name"`
	SinkType                string      `json:"sinkType"`
	Inputs                  string      `json:"inputs"`
	TopicsPattern           string      `json:"topicsPattern"`
	SubsName                string      `json:"subsName"`
	CustomSerdeInputString  string      `json:"customSerdeInputString"`
	CustomSchemaInputString string      `json:"customSchemaInputString"`
	ProcessingGuarantees    string      `json:"processingGuarantees"`
	Archive                 string      `json:"archive"`
	ClassName               string      `json:"className"`
	SinkConfigFile          string      `json:"sinkConfigFile"`
	SinkConfigString        string      `json:"sinkConfigString"`
	InstanceID              string      `json:"instanceId"`
	SinkConf                *SinkConfig `json:"-"`
}

// Topic data
type PartitionedTopicMetadata struct {
	Partitions int `json:"partitions"`
}

type ManagedLedgerInfo struct {
	Version            int                   `json:"version"`
	CreationDate       string                `json:"creationDate"`
	ModificationData   string                `json:"modificationData"`
	Ledgers            []LedgerInfo          `json:"ledgers"`
	TerminatedPosition PositionInfo          `json:"terminatedPosition"`
	Cursors            map[string]CursorInfo `json:"cursors"`
}

type NamespacesData struct {
	Enable                         bool     `json:"enable"`
	Unload                         bool     `json:"unload"`
	NumBundles                     int      `json:"numBundles"`
	BookkeeperEnsemble             int      `json:"bookkeeperEnsemble"`
	BookkeeperWriteQuorum          int      `json:"bookkeeperWriteQuorum"`
	MessageTTL                     int      `json:"messageTTL"`
	BookkeeperAckQuorum            int      `json:"bookkeeperAckQuorum"`
	ManagedLedgerMaxMarkDeleteRate float64  `json:"managedLedgerMaxMarkDeleteRate"`
	ClusterIds                     string   `json:"clusterIds"`
	RetentionTimeStr               string   `json:"retentionTimeStr"`
	LimitStr                       string   `json:"limitStr"`
	PolicyStr                      string   `json:"policyStr"`
	AntiAffinityGroup              string   `json:"antiAffinityGroup"`
	Tenant                         string   `json:"tenant"`
	Cluster                        string   `json:"cluster"`
	Bundle                         string   `json:"bundle"`
	Clusters                       []string `json:"clusters"`
}

type TopicStats struct {
	MsgRateIn           float64                      `json:"msgRateIn"`
	MsgRateOut          float64                      `json:"msgRateOut"`
	MsgThroughputIn     float64                      `json:"msgThroughputIn"`
	MsgThroughputOut    float64                      `json:"msgThroughputOut"`
	AverageMsgSize      float64                      `json:"averageMsgSize"`
	StorageSize         int64                        `json:"storageSize"`
	Publishers          []PublisherStats             `json:"publishers"`
	Subscriptions       map[string]SubscriptionStats `json:"subscriptions"`
	Replication         map[string]ReplicatorStats   `json:"replication"`
	DeDuplicationStatus string                       `json:"deduplicationStatus"`
}

type PublisherStats struct {
	ProducerID      int64             `json:"producerId"`
	MsgRateIn       float64           `json:"msgRateIn"`
	MsgThroughputIn float64           `json:"msgThroughputIn"`
	AverageMsgSize  float64           `json:"averageMsgSize"`
	Metadata        map[string]string `json:"metadata"`
}

type SubscriptionStats struct {
	BlockedSubscriptionOnUnackedMsgs bool            `json:"blockedSubscriptionOnUnackedMsgs"`
	IsReplicated                     bool            `json:"isReplicated"`
	MsgRateOut                       float64         `json:"msgRateOut"`
	MsgThroughputOut                 float64         `json:"msgThroughputOut"`
	MsgRateRedeliver                 float64         `json:"msgRateRedeliver"`
	MsgRateExpired                   float64         `json:"msgRateExpired"`
	MsgBacklog                       int64           `json:"msgBacklog"`
	MsgDelayed                       int64           `json:"msgDelayed"`
	UnAckedMessages                  int64           `json:"unackedMessages"`
	SubType                          string          `json:"type"`
	ActiveConsumerName               string          `json:"activeConsumerName"`
	Consumers                        []ConsumerStats `json:"consumers"`
}

type ConsumerStats struct {
	BlockedConsumerOnUnAckedMsgs bool              `json:"blockedConsumerOnUnackedMsgs"`
	AvailablePermits             int               `json:"availablePermits"`
	UnAckedMessages              int               `json:"unackedMessages"`
	MsgRateOut                   float64           `json:"msgRateOut"`
	MsgThroughputOut             float64           `json:"msgThroughputOut"`
	MsgRateRedeliver             float64           `json:"msgRateRedeliver"`
	ConsumerName                 string            `json:"consumerName"`
	Metadata                     map[string]string `json:"metadata"`
}

type ReplicatorStats struct {
	Connected                 bool    `json:"connected"`
	MsgRateIn                 float64 `json:"msgRateIn"`
	MsgRateOut                float64 `json:"msgRateOut"`
	MsgThroughputIn           float64 `json:"msgThroughputIn"`
	MsgThroughputOut          float64 `json:"msgThroughputOut"`
	MsgRateExpired            float64 `json:"msgRateExpired"`
	ReplicationBacklog        int64   `json:"replicationBacklog"`
	ReplicationDelayInSeconds int64   `json:"replicationDelayInSeconds"`
	InboundConnection         string  `json:"inboundConnection"`
	InboundConnectedSince     string  `json:"inboundConnectedSince"`
	OutboundConnection        string  `json:"outboundConnection"`
	OutboundConnectedSince    string  `json:"outboundConnectedSince"`
}

type PersistentTopicInternalStats struct {
	WaitingCursorsCount                int                    `json:"waitingCursorsCount"`
	PendingAddEntriesCount             int                    `json:"pendingAddEntriesCount"`
	EntriesAddedCounter                int64                  `json:"entriesAddedCounter"`
	NumberOfEntries                    int64                  `json:"numberOfEntries"`
	TotalSize                          int64                  `json:"totalSize"`
	CurrentLedgerEntries               int64                  `json:"currentLedgerEntries"`
	CurrentLedgerSize                  int64                  `json:"currentLedgerSize"`
	LastLedgerCreatedTimestamp         string                 `json:"lastLedgerCreatedTimestamp"`
	LastLedgerCreationFailureTimestamp string                 `json:"lastLedgerCreationFailureTimestamp"`
	LastConfirmedEntry                 string                 `json:"lastConfirmedEntry"`
	State                              string                 `json:"state"`
	Ledgers                            []LedgerInfo           `json:"ledgers"`
	Cursors                            map[string]CursorStats `json:"cursors"`
}

type LedgerInfo struct {
	LedgerID  int64 `json:"ledgerId"`
	Entries   int64 `json:"entries"`
	Size      int64 `json:"size"`
	Offloaded bool  `json:"offloaded"`
}

type CursorInfo struct {
	Version                   int                `json:"version"`
	CreationDate              string             `json:"creationDate"`
	ModificationDate          string             `json:"modificationDate"`
	CursorsLedgerID           int64              `json:"cursorsLedgerId"`
	MarkDelete                PositionInfo       `json:"markDelete"`
	IndividualDeletedMessages []MessageRangeInfo `json:"individualDeletedMessages"`
	Properties                map[string]int64
}

type PositionInfo struct {
	LedgerID int64 `json:"ledgerId"`
	EntryID  int64 `json:"entryId"`
}

type MessageRangeInfo struct {
	From      PositionInfo `json:"from"`
	To        PositionInfo `json:"to"`
	Offloaded bool         `json:"offloaded"`
}

type CursorStats struct {
	MarkDeletePosition                       string           `json:"markDeletePosition"`
	ReadPosition                             string           `json:"readPosition"`
	WaitingReadOp                            bool             `json:"waitingReadOp"`
	PendingReadOps                           int              `json:"pendingReadOps"`
	MessagesConsumedCounter                  int64            `json:"messagesConsumedCounter"`
	CursorLedger                             int64            `json:"cursorLedger"`
	CursorLedgerLastEntry                    int64            `json:"cursorLedgerLastEntry"`
	IndividuallyDeletedMessages              string           `json:"individuallyDeletedMessages"`
	LastLedgerWitchTimestamp                 string           `json:"lastLedgerWitchTimestamp"`
	State                                    string           `json:"state"`
	NumberOfEntriesSinceFirstNotAckedMessage int64            `json:"numberOfEntriesSinceFirstNotAckedMessage"`
	TotalNonContiguousDeletedMessagesRange   int              `json:"totalNonContiguousDeletedMessagesRange"`
	Properties                               map[string]int64 `json:"properties"`
}

type PartitionedTopicStats struct {
	MsgRateIn           float64                      `json:"msgRateIn"`
	MsgRateOut          float64                      `json:"msgRateOut"`
	MsgThroughputIn     float64                      `json:"msgThroughputIn"`
	MsgThroughputOut    float64                      `json:"msgThroughputOut"`
	AverageMsgSize      float64                      `json:"averageMsgSize"`
	StorageSize         int64                        `json:"storageSize"`
	Publishers          []PublisherStats             `json:"publishers"`
	Subscriptions       map[string]SubscriptionStats `json:"subscriptions"`
	Replication         map[string]ReplicatorStats   `json:"replication"`
	DeDuplicationStatus string                       `json:"deduplicationStatus"`
	Metadata            PartitionedTopicMetadata     `json:"metadata"`
	Partitions          map[string]TopicStats        `json:"partitions"`
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
	BrokerURL    string `json:"brokerUrl"`
	BrokerURLTLS string `json:"brokerUrlTls"`
	HTTPURL      string `json:"httpUrl"`
	HTTPURLTLS   string `json:"httpUrlTls"`
}

type NsIsolationPoliciesData struct {
	Namespaces                 []string `json:"namespaces"`
	Primary                    []string `json:"primary"`
	Secondary                  []string `json:"secondary"`
	AutoFailoverPolicyTypeName string   `json:"autoFailoverPolicyTypeName"`
	AutoFailoverPolicyParams   string   `json:"autoFailoverPolicyParams"`
}

type BrokerData struct {
	URL         string `json:"brokerUrl"`
	ConfigName  string `json:"configName"`
	ConfigValue string `json:"configValue"`
}

type BrokerStatsData struct {
	Indent bool `json:"indent"`
}

type ResourceQuotaData struct {
	Names        string `json:"names"`
	Bundle       string `json:"bundle"`
	MsgRateIn    int64  `json:"msgRateIn"`
	MsgRateOut   int64  `json:"msgRateOut"`
	BandwidthIn  int64  `json:"bandwidthIn"`
	BandwidthOut int64  `json:"bandwidthOut"`
	Memory       int64  `json:"memory"`
	Dynamic      bool   `json:"dynamic"`
}
