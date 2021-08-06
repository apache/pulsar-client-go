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
	Tenant                   string  `json:"tenant,omitempty"`
	Namespace                string  `json:"namespace,omitempty"`
	Name                     string  `json:"name,omitempty"`
	SourceType               string  `json:"sourceType,omitempty"`
	ProcessingGuarantees     string  `json:"processingGuarantees,omitempty"`
	DestinationTopicName     string  `json:"destinationTopicName,omitempty"`
	DeserializationClassName string  `json:"deserializationClassName,omitempty"`
	SchemaType               string  `json:"schemaType,omitempty"`
	Parallelism              int     `json:"parallelism,omitempty"`
	Archive                  string  `json:"archive,omitempty"`
	ClassName                string  `json:"className,omitempty"`
	SourceConfigFile         string  `json:"sourceConfigFile,omitempty"`
	CPU                      float64 `json:"cpu,omitempty"`
	RAM                      int64   `json:"ram,omitempty"`
	Disk                     int64   `json:"disk,omitempty"`
	SourceConfigString       string  `json:"sourceConfigString,omitempty"`

	SourceConf *SourceConfig `json:"-,omitempty"`
	InstanceID string        `json:"instanceId,omitempty"`

	UpdateAuthData bool `json:"updateAuthData,omitempty"`
}

type SinkData struct {
	UpdateAuthData          bool        `json:"updateAuthData,omitempty"`
	RetainOrdering          bool        `json:"retainOrdering,omitempty"`
	AutoAck                 bool        `json:"autoAck,omitempty"`
	Parallelism             int         `json:"parallelism,omitempty"`
	RAM                     int64       `json:"ram,omitempty"`
	Disk                    int64       `json:"disk,omitempty"`
	TimeoutMs               int64       `json:"timeoutMs,omitempty"`
	CPU                     float64     `json:"cpu,omitempty"`
	Tenant                  string      `json:"tenant,omitempty"`
	Namespace               string      `json:"namespace,omitempty"`
	Name                    string      `json:"name,omitempty"`
	SinkType                string      `json:"sinkType,omitempty"`
	Inputs                  string      `json:"inputs,omitempty"`
	TopicsPattern           string      `json:"topicsPattern,omitempty"`
	SubsName                string      `json:"subsName,omitempty"`
	CustomSerdeInputString  string      `json:"customSerdeInputString,omitempty"`
	CustomSchemaInputString string      `json:"customSchemaInputString,omitempty"`
	ProcessingGuarantees    string      `json:"processingGuarantees,omitempty"`
	Archive                 string      `json:"archive,omitempty"`
	ClassName               string      `json:"className,omitempty"`
	SinkConfigFile          string      `json:"sinkConfigFile,omitempty"`
	SinkConfigString        string      `json:"sinkConfigString,omitempty"`
	InstanceID              string      `json:"instanceId,omitempty"`
	SinkConf                *SinkConfig `json:"-,omitempty"`
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
	LimitTime                      int64    `json:"limitTime"`
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
	Timestamp int64 `json:"timestamp"`
	Offloaded bool  `json:"isOffloaded"`
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

type PersistenceData struct {
	BookkeeperEnsemble             int64   `json:"bookkeeperEnsemble"`
	BookkeeperWriteQuorum          int64   `json:"bookkeeperWriteQuorum"`
	BookkeeperAckQuorum            int64   `json:"bookkeeperAckQuorum"`
	ManagedLedgerMaxMarkDeleteRate float64 `json:"managedLedgerMaxMarkDeleteRate"`
}

type DelayedDeliveryCmdData struct {
	Enable                 bool   `json:"enable"`
	Disable                bool   `json:"disable"`
	DelayedDeliveryTimeStr string `json:"delayedDeliveryTimeStr"`
}

type DelayedDeliveryData struct {
	TickTime float64 `json:"tickTime"`
	Active   bool    `json:"active"`
}

type DispatchRateData struct {
	DispatchThrottlingRateInMsg  int64 `json:"dispatchThrottlingRateInMsg"`
	DispatchThrottlingRateInByte int64 `json:"dispatchThrottlingRateInByte"`
	RatePeriodInSecond           int64 `json:"ratePeriodInSecond"`
	RelativeToPublishRate        bool  `json:"relativeToPublishRate"`
}
