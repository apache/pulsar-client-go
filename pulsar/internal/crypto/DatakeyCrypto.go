package crypto

// DataKeyCrypto : implement this interface to provide custom encryption and decryption for data key
// One example is that one want to encrypt the data key using Key managenet service(KMS) i.e AWS KMS etc
type DataKeyCrypto interface {
	// EncryptDataKey encrypt data key
	EncryptDataKey(dataKey []byte) ([]byte, error)

	// DecryptDataKey decrypt data key
	DecryptDataKey(dataKey []byte) ([]byte, error)
}
