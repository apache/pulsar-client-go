package impl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTopicName(t *testing.T) {
	topic, err := ParseTopicName("persistent://my-tenant/my-ns/my-topic")

	assert.Nil(t, err)
	assert.Equal(t, "persistent://my-tenant/my-ns/my-topic", topic.Name)
	assert.Equal(t, "my-tenant/my-ns", topic.Namespace)
	assert.Equal(t, -1, topic.Partition)

	topic, err = ParseTopicName("my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public/default/my-topic", topic.Name)
	assert.Equal(t, "public/default", topic.Namespace)
	assert.Equal(t, -1, topic.Partition)

	topic, err = ParseTopicName("my-tenant/my-namespace/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://my-tenant/my-namespace/my-topic", topic.Name)
	assert.Equal(t, "my-tenant/my-namespace", topic.Namespace)
	assert.Equal(t, -1, topic.Partition)

	topic, err = ParseTopicName("non-persistent://my-tenant/my-namespace/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "non-persistent://my-tenant/my-namespace/my-topic", topic.Name)
	assert.Equal(t, "my-tenant/my-namespace", topic.Namespace)
	assert.Equal(t, -1, topic.Partition)


	topic, err = ParseTopicName("my-topic-partition-5")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public/default/my-topic-partition-5", topic.Name)
	assert.Equal(t, "public/default", topic.Namespace)
	assert.Equal(t, 5, topic.Partition)

	// V1 topic name
	topic, err = ParseTopicName("persistent://my-tenant/my-cluster/my-ns/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://my-tenant/my-cluster/my-ns/my-topic", topic.Name)
	assert.Equal(t, "my-tenant/my-cluster/my-ns", topic.Namespace)
	assert.Equal(t, -1, topic.Partition)
}

func TestParseTopicNameErrors(t *testing.T) {
	_, err := ParseTopicName("invalid://my-tenant/my-ns/my-topic")
	assert.NotNil(t, err)

	_, err = ParseTopicName("invalid://my-tenant/my-ns/my-topic-partition-xyz")
	assert.NotNil(t, err)

	_, err = ParseTopicName("my-tenant/my-ns/my-topic-partition-xyz/invalid")
	assert.NotNil(t, err)

	_, err = ParseTopicName("persistent://my-tenant")
	assert.NotNil(t, err)

	_, err = ParseTopicName("persistent://my-tenant/my-namespace")
	assert.NotNil(t, err)

	_, err = ParseTopicName("persistent://my-tenant/my-cluster/my-ns/my-topic-partition-xyz/invalid")
	assert.NotNil(t, err)
}
