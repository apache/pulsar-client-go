package pulsar

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_joinErrors(t *testing.T) {
	err1 := errors.New("err1")
	err2 := errors.New("err2")
	err3 := errors.New("err3")
	err := joinErrors(ErrInvalidMessage, err1, err2)
	assert.True(t, errors.Is(err, ErrInvalidMessage))
	assert.True(t, errors.Is(err, err1))
	assert.True(t, errors.Is(err, err2))
	assert.False(t, errors.Is(err, err3))
}
