package compression

type noopProvider struct {
}

func NewNoopProvider() Provider {
	return &noopProvider{}
}

func (noopProvider) Compress(data []byte) []byte {
	return data
}

func (noopProvider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	return compressedData, nil
}
