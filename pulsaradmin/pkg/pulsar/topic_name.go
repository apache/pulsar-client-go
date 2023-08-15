package pulsar

import (
	"fmt"
	"github.com/pkg/errors"
	"net/url"
	"strconv"
	"strings"
)

const (
	PUBLIC_TENANT            = "public"
	DEFAULT_NAMESPACE        = "default"
	PARTITIONED_TOPIC_SUFFIX = "-partition-"
)

type TopicName struct {
	domain         TopicDomain
	tenant         string
	namespace      string
	topic          string
	partitionIndex int

	namespaceName *NameSpaceName
}

// The topic name can be in two different forms, one is fully qualified topic name,
// the other one is short topic name
func GetTopicName(completeName string) (*TopicName, error) {
	var topicname TopicName
	// The short topic name can be:
	// - <topic>
	// - <tenant>/<namespace>/<topic>
	if !strings.Contains(completeName, "://") {
		parts := strings.Split(completeName, "/")
		if len(parts) == 3 {
			completeName = persistent.String() + "://" + completeName
		} else if len(parts) == 1 {
			completeName = persistent.String() + "://" + PUBLIC_TENANT + "/" + DEFAULT_NAMESPACE + "/" + parts[0]
		} else {
			return nil, errors.Errorf("Invalid short topic name '%s', it should be "+
				"in the format of <tenant>/<namespace>/<topic> or <topic>", completeName)
		}
	}

	// The fully qualified topic name can be:
	// <domain>://<tenant>/<namespace>/<topic>

	parts := strings.SplitN(completeName, "://", 2)

	domain, err := ParseTopicDomain(parts[0])
	if err != nil {
		return nil, err
	}
	topicname.domain = domain

	rest := parts[1]
	parts = strings.SplitN(rest, "/", 3)
	if len(parts) == 3 {
		topicname.tenant = parts[0]
		topicname.namespace = parts[1]
		topicname.topic = parts[2]
		topicname.partitionIndex = getPartitionIndex(completeName)
	} else {
		return nil, errors.Errorf("Invalid topic name '%s', it should be in the format of "+
			"<tenant>/<namespace>/<topic>", rest)
	}

	if topicname.topic == "" {
		return nil, errors.New("Topic name can not be empty.")
	}

	n, err := GetNameSpaceName(topicname.tenant, topicname.namespace)
	if err != nil {
		return nil, err
	}
	topicname.namespaceName = n

	return &topicname, nil
}

func (t *TopicName) String() string {
	return fmt.Sprintf("%s://%s/%s/%s", t.domain, t.tenant, t.namespace, t.topic)
}

func (t *TopicName) GetDomain() TopicDomain {
	return t.domain
}

func (t *TopicName) GetRestPath() string {
	return fmt.Sprintf("%s/%s/%s/%s", t.domain, t.tenant, t.namespace, t.GetEncodedTopic())
}

func (t *TopicName) GetEncodedTopic() string {
	return url.QueryEscape(t.topic)
}

func (t *TopicName) GetLocalName() string {
	return t.topic
}

func (t *TopicName) GetPartition(index int) (*TopicName, error) {
	if index < 0 {
		return nil, errors.New("Invalid partition index number.")
	}

	if strings.Contains(t.String(), PARTITIONED_TOPIC_SUFFIX) {
		return t, nil
	}

	topicNameWithPartition := t.String() + PARTITIONED_TOPIC_SUFFIX + strconv.Itoa(index)
	return GetTopicName(topicNameWithPartition)
}

func getPartitionIndex(topic string) int {
	if strings.Contains(topic, PARTITIONED_TOPIC_SUFFIX) {
		parts := strings.Split(topic, "-")
		index, err := strconv.Atoi(parts[len(parts)-1])
		if err == nil {
			return index
		}
	}
	return -1
}
