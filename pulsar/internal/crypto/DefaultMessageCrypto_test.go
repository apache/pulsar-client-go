package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"testing"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
)

type testDataKeyCrypto struct {
	nonce []byte
	key   []byte
}

func (t *testDataKeyCrypto) EncryptDataKey(dataKey []byte) ([]byte, error) {

	gcm, err := t.getGCM()
	if err != nil {
		return nil, err
	}

	t.nonce = make([]byte, gcm.NonceSize())
	fmt.Println(gcm.NonceSize())

	_, err = rand.Read(t.nonce)
	if err != nil {
		return nil, err
	}
	encryptedKey := gcm.Seal(nil, t.nonce, dataKey, nil)
	return encryptedKey, nil
}

func (t *testDataKeyCrypto) DecryptDataKey(dataKey []byte) ([]byte, error) {
	gcm, err := t.getGCM()
	if err != nil {
		return nil, err
	}
	decryptedKey, err := gcm.Open(nil, t.nonce, dataKey, nil)
	return decryptedKey, err
}

func (t *testDataKeyCrypto) getGCM() (cipher.AEAD, error) {
	c, err := aes.NewCipher(t.key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	return gcm, nil
}

func TestEncryptDecrypt(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}

	nonce := make([]byte, 12)
	_, err := rand.Read(nonce)
	if err != nil {
		t.Error(err)
	}

	var dataKeyCrypto DataKeyCrypto = &testDataKeyCrypto{
		nonce: nonce,
		key:   []byte("passphrasewhichneedstobe32bytes!"),
	}

	testdata := []byte("My Super Secret Code Stuff!")

	defaultMessageCrypto_producer, err := NewDefaultMessageCrypto("testing", true, log.DefaultNopLogger())
	if err != nil {
		t.Error(err)
	}

	encryptedData, err := defaultMessageCrypto_producer.Encrypt([]string{"my-encryption-key-01"}, dataKeyCrypto, msgMetadata, testdata)
	if err != nil {
		t.Error(err)
	}

	defaultMessageCrypto_consumer, err := NewDefaultMessageCrypto("testing", false, log.DefaultNopLogger())

	if err != nil {
		t.Error(err)
	}

	decryptedData, err := defaultMessageCrypto_consumer.Decrypt(msgMetadata, encryptedData, dataKeyCrypto)

	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, testdata, decryptedData)
}
