package crypto

import (
	"crypto/aes"
	"crypto/cipher"
)

// DefaultDataKeyCrypto used just for the testing purpose and should not use it in production
type DefaultDataKeyCrypto struct {
	Nonce []byte
	Key   []byte
}

// NewDefaultDataKeyCrypto default datakey crypto
func NewDefaultDataKeyCrypto() DataKeyCrypto {
	return &DefaultDataKeyCrypto{
		Nonce: []byte{203, 42, 246, 196, 4, 32, 73, 203, 242, 64, 233, 39},
		Key:   []byte("passphrasewhichneedstobe32bytes!"),
	}
}

// EncryptDataKey encrypt the data key
func (t *DefaultDataKeyCrypto) EncryptDataKey(dataKey []byte) ([]byte, error) {

	gcm, err := t.getGCM()
	if err != nil {
		return nil, err
	}

	encryptedKey := gcm.Seal(nil, t.Nonce, dataKey, nil)
	return encryptedKey, nil
}

// DecryptDataKey decrypt the data key
func (t *DefaultDataKeyCrypto) DecryptDataKey(dataKey []byte) ([]byte, error) {
	gcm, err := t.getGCM()
	if err != nil {
		return nil, err
	}
	decryptedKey, err := gcm.Open(nil, t.Nonce, dataKey, nil)
	return decryptedKey, err
}

func (t *DefaultDataKeyCrypto) getGCM() (cipher.AEAD, error) {
	c, err := aes.NewCipher(t.Key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	return gcm, nil
}
