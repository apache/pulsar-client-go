package crypto

import "io/ioutil"

// DefaultCryptoKeyReader default implementation of CryptoKeyReader
type DefaultCryptoKeyReader struct {
	publicKeyPath  string
	privateKeyPath string
}

func NewDefaultCryptoKeyReader(publicKeyPath, privateKeyPath string) *DefaultCryptoKeyReader {
	return &DefaultCryptoKeyReader{
		publicKeyPath:  publicKeyPath,
		privateKeyPath: privateKeyPath,
	}
}

// GetPublicKey read public key from the given path
func (d *DefaultCryptoKeyReader) GetPublicKey(keyName string, keyMeta map[string]string) (*EncryptionKeyInfo, error) {
	return readKey(d.publicKeyPath, keyMeta)
}

// GetPrivateKey read private key from the given path
func (d *DefaultCryptoKeyReader) GetPrivateKey(keyName string, keyMeta map[string]string) (*EncryptionKeyInfo, error) {
	return readKey(d.privateKeyPath, keyMeta)
}

func readKey(path string, keyMeta map[string]string) (*EncryptionKeyInfo, error) {
	key, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return NewEncryptionKeyInfo(key, keyMeta), nil
}
