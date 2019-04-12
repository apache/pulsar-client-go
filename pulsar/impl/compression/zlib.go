package compression

import (
	"bytes"
	"compress/zlib"
)

type zlibProvider struct {
}

func NewZLibProvider() Provider {
	return &zlibProvider{}
}

func (zlibProvider) Compress(data []byte) []byte {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write(data)
	w.Close()

	return b.Bytes()
}

func (zlibProvider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewBuffer(compressedData))
	if err != nil {
		return nil, err
	}

	uncompressed := make([]byte, originalSize)
	r.Read(uncompressed)
	r.Close()

	return uncompressed, nil
}
