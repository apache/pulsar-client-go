package crypto

// CryptoKeyReader implement this interface to read and provide public & private keys
// key pair can be RSA, ECDSA
type CryptoKeyReader interface {
	// get public key that is be used by the producer to encrypt data key
	getPublicKey(keyName string, metadata map[string]string) EncryptionKeyInfo

	// get private key that is used by the consumer to decrypt data key
	getPrivateKey(keyName string, metadata map[string]string) EncryptionKeyInfo
}
