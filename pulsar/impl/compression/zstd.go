package compression

import (
	zstd "github.com/valyala/gozstd"
)

type zstdProvider struct {
}

func NewZStdProvider() Provider {
	return &zstdProvider{}
}

func (zstdProvider) Compress(data []byte) []byte {
	return zstd.Compress(nil, data)
}

func (zstdProvider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	return zstd.Decompress(nil, compressedData)
}
