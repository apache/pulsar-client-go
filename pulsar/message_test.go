package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageZeroEventTime(t *testing.T) {
	msg := &ProducerMessage{}
	assert.Equal(t, false, msg.EventTime.UnixNano() == 0)
	assert.Equal(t, true, msg.EventTime.IsZero())
}
