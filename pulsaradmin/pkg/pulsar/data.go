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

	FuncConf     *FunctionConfig `json:"-"`
	UserCodeFile string          `json:"-"`
}
