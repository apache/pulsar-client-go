package pulsar

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessageIDFromString(t *testing.T) {
	id, err := MessageIDFromString("1:2")
	assert.Nil(t, err)
	assert.True(t, id.Equals(MessageIDFromParts(1, 2, -1,  -1)))

	id, err = MessageIDFromString("1:2:3")
	assert.Nil(t, err)
	assert.True(t, id.Equals(MessageIDFromParts(1, 2, -1,  3)))

	id, err = MessageIDFromString("1:2:3:4")
	assert.Nil(t, err)
	assert.True(t, id.Equals(MessageIDFromParts(1, 2, 4,  3)))
}

func TestMessageIDFromStringErrors(t *testing.T) {
	id, err := MessageIDFromString("1;1")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid message id string. 1;1", err.Error())

	id, err = MessageIDFromString("a:1")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid ledger id. a:1", err.Error())

	id, err = MessageIDFromString("1:a")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid entry id. 1:a", err.Error())

	id, err = MessageIDFromString("1:2:a")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid partition index. 1:2:a", err.Error())

	id, err = MessageIDFromString("1:2:3:a")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid batch index. 1:2:3:a", err.Error())
}
