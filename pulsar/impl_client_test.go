package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	client, err := NewClient(ClientOptions{})
	assert.Nil(t, client)
	assert.NotNil(t, err)
	assert.Equal(t, Result(ResultInvalidConfiguration), err.(*Error).Result())
}
