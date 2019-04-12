package compression

import (
	"github.com/cloudflare/golz4"
)

type lz4Provider struct {
}

func NewLz4Provider() Provider {
	return &lz4Provider{}
}

func (lz4Provider) Compress(data []byte) []byte {
	maxSize := lz4.CompressBound(data)
	compressed := make([]byte, maxSize)
	size, err := lz4.Compress(data, compressed)
	if err != nil {
		panic("Failed to compress")
	}
	return compressed[:size]
}

func (lz4Provider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	uncompressed := make([]byte, originalSize)
	err := lz4.Uncompress(compressedData, uncompressed)
	return uncompressed, err
}
