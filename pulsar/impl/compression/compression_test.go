package compression

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testProvider struct {
	name     string
	provider Provider

	// Compressed data for "hello"
	compressedHello []byte
}

var providers = []testProvider{
	{"zlib", ZLibProvider, []byte{0x78, 0x9c, 0xca, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00, 0x00, 0xff, 0xff}},
	{"lz4", Lz4Provider, []byte{0x50, 0x68, 0x65, 0x6c, 0x6c, 0x6f}},
	{"zstd", ZStdProvider, []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x05, 0x29, 0x00, 0x00, 0x68, 0x65, 0x6c, 0x6c, 0x6f}},
}

func TestCompression(t *testing.T) {
	for _, p := range providers {
		t.Run(p.name, func(t *testing.T) {
			hello := []byte("test compression data")
			compressed := p.provider.Compress(hello)
			uncompressed, err := p.provider.Decompress(compressed, len(hello))
			assert.Nil(t, err)
			assert.ElementsMatch(t, hello, uncompressed)
		})
	}
}

func TestJavaCompatibility(t *testing.T) {
	for _, p := range providers {
		t.Run(p.name, func(t *testing.T) {
			hello := []byte("hello")
			uncompressed, err := p.provider.Decompress(p.compressedHello, len(hello))
			assert.Nil(t, err)
			assert.ElementsMatch(t, hello, uncompressed)
		})
	}
}

func TestDecompressionError(t *testing.T) {
	for _, p := range providers {
		t.Run(p.name, func(t *testing.T) {
			_, err := p.provider.Decompress([]byte{0x05}, 0)
			assert.NotNil(t, err)
		})
	}
}
