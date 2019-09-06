package pulsar

import (
	"github.com/stretchr/testify/assert"
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

	fail, err := GetTopicName("default/fail")
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
	assert.Equal(t, "Invalid topic name '/tenant/namespace/fail', it should be in the format "+
		"of <tenant>/<namespace>/<topic>", err.Error())
	assert.Nil(t, fail)

	fail, err = GetTopicName("persistent://tenant/namespace")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid topic name 'tenant/namespace', it should be in the format "+
		"of <tenant>/<namespace>/<topic>", err.Error())
	assert.Nil(t, fail)
}
