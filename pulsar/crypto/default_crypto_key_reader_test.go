package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPublicKey(t *testing.T) {
	keyReader := NewFileKeyReader("../crypto/testdata/pub_key_rsa.pem", "")
	keyInfo, err := keyReader.PublicKey("test-key", map[string]string{"key": "value"})

	assert.Nil(t, err)
	assert.NotNil(t, keyInfo)
	assert.NotEmpty(t, keyInfo.Metadata())
	assert.NotEmpty(t, keyInfo.Name())
	assert.NotEmpty(t, keyInfo.Key())
	assert.Equal(t, "value", keyInfo.metadata["key"])
}

func TestGetPrivateKey(t *testing.T) {
	keyReader := NewFileKeyReader("", "../crypto/testdata/pri_key_rsa.pem")
	keyInfo, err := keyReader.PrivateKey("test-key", map[string]string{"key": "value"})

	assert.Nil(t, err)
	assert.NotNil(t, keyInfo)
	assert.NotEmpty(t, keyInfo.Metadata())
	assert.NotEmpty(t, keyInfo.Name())
	assert.NotEmpty(t, keyInfo.Key())
	assert.Equal(t, "value", keyInfo.metadata["key"])
}

func TestInvalidKeyPath(t *testing.T) {
	keyReader := NewFileKeyReader("../crypto/testdata/no_pub_key_rsa.pem", "../crypto/testdata/no_pri_key_rsa.pem")

	// try to read public key
	keyInfo, err := keyReader.PublicKey("test-pub-key", nil)
	assert.Nil(t, keyInfo)
	assert.NotNil(t, err)

	// try to read private key
	keyInfo, err = keyReader.PrivateKey("test-pri-key", nil)
	assert.Nil(t, keyInfo)
	assert.NotNil(t, err)
}
