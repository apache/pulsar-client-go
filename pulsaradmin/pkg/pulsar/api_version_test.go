package pulsar

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestApiVersion_String(t *testing.T) {
	assert.Equal(t, "v1", V1.String())
	assert.Equal(t, "v2", V2.String())
	assert.Equal(t, "v3", V3.String())
}