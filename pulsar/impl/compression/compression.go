package compression

type Provider interface {
	Compress(data []byte) []byte

	Decompress(compressedData []byte, originalSize int) ([]byte, error)
}

var (
	NoopProvider = NewNoopProvider()
	ZLibProvider = NewZLibProvider()
	Lz4Provider  = NewLz4Provider()
	ZStdProvider = NewZStdProvider()
)
