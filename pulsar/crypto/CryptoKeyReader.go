package crypto

// CryptoKeyReader implement this interface to read and provide public & private keys
// key pair can be RSA, ECDSA
type CryptoKeyReader interface {
	// GetPublicKey get public key that is be used by the producer to encrypt data key
	GetPublicKey(keyName string, metadata map[string]string) (*EncryptionKeyInfo, error)

	// GetPrivateKey get private key that is used by the consumer to decrypt data key
	GetPrivateKey(keyName string, metadata map[string]string) (*EncryptionKeyInfo, error)
}
