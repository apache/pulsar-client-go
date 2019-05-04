package impl

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testProvider struct {
	str string

	hash uint32
}

var javaHashValues = []testProvider{
	{"", 0x0,},
	{"hello", 0x5e918d2},
	{"test", 0x364492},
}

var murmurHashValues = []testProvider{
	{"", 0x0},
	{"hello", 0x248bfa47},
	{"test", 0x3a6bd213},
}

func TestJavaHash(t *testing.T) {
	for _, p := range javaHashValues {
		t.Run(p.str, func(t *testing.T) {
			assert.Equal(t, p.hash, JavaStringHash(p.str))
		})
	}
}

func TestMurmurHash(t *testing.T) {
	for _, p := range murmurHashValues {
		t.Run(p.str, func(t *testing.T) {
			assert.Equal(t, p.hash, Murmur3_32Hash(p.str))
		})
	}
}
