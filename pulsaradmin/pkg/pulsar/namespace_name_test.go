package pulsar

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetNamespaceName(t *testing.T) {
	success, err := GetNamespaceName("public/default")
	assert.Nil(t, err)
	assert.Equal(t, "public/default", success.String())

	empty, err := GetNamespaceName("")
	assert.NotNil(t, err)
	assert.Equal(t, "The namespace complete name is empty.", err.Error())
	assert.Nil(t, empty)

	empty, err = GetNamespaceName("/")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid tenant or namespace. [/]", err.Error())
	assert.Nil(t, empty)

	invalid, err := GetNamespaceName("public/default/fail")
	assert.NotNil(t, err)
	assert.Equal(t, "The complete name of namespace is invalid. complete name : [public/default/fail]", err.Error())
	assert.Nil(t, invalid)

	invalid, err = GetNamespaceName("public")
	assert.NotNil(t, err)
	assert.Equal(t, "The complete name of namespace is invalid. complete name : [public]", err.Error())
	assert.Nil(t, invalid)

	special, err := GetNamespaceName("-=.:/-=.:")
	assert.Nil(t, err)
	assert.Equal(t, "-=.:/-=.:", special.String())

	tenantInvalid, err := GetNamespaceName("\"/namespace")
	assert.NotNil(t, err)
	assert.Equal(t, "Tenant name include unsupported special chars. tenant : [\"]", err.Error())
	assert.Nil(t, tenantInvalid)

	namespaceInvalid, err := GetNamespaceName("tenant/}")
	assert.NotNil(t, err)
	assert.Equal(t, "Namespace name include unsupported special chars. namespace : [}]", err.Error())
	assert.Nil(t, namespaceInvalid)
}
