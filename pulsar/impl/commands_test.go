package impl

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConvertStringMap(t *testing.T) {
	m := make(map[string]string)
	m["a"] = "1"
	m["b"] = "2"

	pbm := ConvertFromStringMap(m)

	assert.Equal(t, 2, len(pbm))
	assert.Equal(t, "a", *pbm[0].Key)
	assert.Equal(t, "1", *pbm[0].Value)
	assert.Equal(t, "b", *pbm[1].Key)
	assert.Equal(t, "2", *pbm[1].Value)

	m2 := ConvertToStringMap(pbm)
	assert.Equal(t, 2, len(m2))
	assert.Equal(t, "1", m2["a"])
	assert.Equal(t, "2", m2["b"])
}

