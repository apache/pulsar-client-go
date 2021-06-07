package crypto

// EncryptionKeyInfo
type EncryptionKeyInfo struct {
	metadata map[string]string
	key      []byte
}

// NewEncryptionKeyInfo
func NewEncryptionKeyInfo(key []byte, metadata map[string]string) *EncryptionKeyInfo {
	return &EncryptionKeyInfo{
		metadata: metadata,
		key:      key,
	}
}

// GetKey get key
func (eci *EncryptionKeyInfo) GetKey() []byte {
	return eci.key
}

// SetKey set key
func (eci *EncryptionKeyInfo) SetKey(key []byte) {
	eci.key = key
}

// GetMetadata get key metadata
func (eci *EncryptionKeyInfo) GetMetadata() map[string]string {
	return eci.metadata
}

// SetMetadata set key metadata
func (eci *EncryptionKeyInfo) SetMetadata(metadata map[string]string) {
	eci.metadata = metadata
}
