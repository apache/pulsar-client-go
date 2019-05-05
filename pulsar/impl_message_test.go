package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageId(t *testing.T) {
	id := newMessageId(1,2, 3, 4)
	bytes := id.Serialize()

	id2, err := DeserializeMessageID(bytes)
	assert.NoError(t, err)
	assert.NotNil(t, id2)

	assert.Equal(t, int64(1), id2.(*messageId).ledgerID)
	assert.Equal(t, int64(2), id2.(*messageId).entryID)
	assert.Equal(t, 3, id2.(*messageId).batchIdx)
	assert.Equal(t, 4, id2.(*messageId).partitionIdx)

	id, err = DeserializeMessageID(nil)
	assert.Error(t, err)
	assert.Nil(t, id)

	id, err = DeserializeMessageID(make([]byte, 0))
	assert.Error(t, err)
	assert.Nil(t, id)
}
