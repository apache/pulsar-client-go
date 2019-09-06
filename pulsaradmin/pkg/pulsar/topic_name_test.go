package pulsar

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestGetTopicName(t *testing.T) {
	success, err := GetTopicName("success")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public/default/success", success.String())

	success, err = GetTopicName("tenant/namespace/success")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://tenant/namespace/success", success.String())

	success, err = GetTopicName("persistent://tenant/namespace/success")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://tenant/namespace/success", success.String())

	success, err = GetTopicName("non-persistent://tenant/namespace/success")
	assert.Nil(t, err)
	assert.Equal(t, "non-persistent://tenant/namespace/success", success.String())

	fail, err := GetTopicName("://tenant.namespace.topic")
	assert.NotNil(t, err)
	assert.Equal(t, "The domain only can be specified as 'persistent' or 'non-persistent'. Input domain is ''.", err.Error())

	fail, err = GetTopicName("default/fail")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid short topic name 'default/fail', it should be in the "+
		"format of <tenant>/<namespace>/<topic> or <topic>", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("domain://tenant/namespace/fail")
	assert.NotNil(t, err)
	assert.Equal(t, "The domain only can be specified as 'persistent' or 'non-persistent'. "+
		"Input domain is 'domain'.", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("persistent:///tenant/namespace/fail")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid tenant or namespace. [/tenant]", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("persistent://tenant/namespace")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid topic name 'tenant/namespace', it should be in the format "+
		"of <tenant>/<namespace>/<topic>", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("persistent://tenant/namespace/")
	assert.NotNil(t, err)
	assert.Equal(t, "Topic name can not be empty.", err.Error())
	assert.Nil(t, fail)
}

func TestTopicNameEncodeTest(t *testing.T) {
	encodedName := "a%3Aen-in_in_business_content_item_20150312173022_https%5C%3A%2F%2Fin.news.example.com%2Fr"
	rawName := "a:en-in_in_business_content_item_20150312173022_https\\://in.news.example.com/r"

	assert.Equal(t, encodedName, url.QueryEscape(rawName))
	o, err := url.QueryUnescape(encodedName)
	assert.Nil(t, err)
	assert.Equal(t, rawName, o)

	topicName, err := GetTopicName("persistent://prop/ns/" + rawName)
	assert.Nil(t, err)

	assert.Equal(t, rawName, topicName.topic)
	assert.Equal(t, encodedName, topicName.GetEncodedTopic())
	assert.Equal(t, "persistent/prop/ns/"+encodedName, topicName.GetRestPath())
}
