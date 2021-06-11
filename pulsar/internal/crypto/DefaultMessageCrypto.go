package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// DefaultMessageCrypto implmentation of the interface MessageCryto
type DefaultMessageCrypto struct {
	// data key which is used to encrypt/decrypt messages
	dataKey []byte

	// LoadingCache used by the consumer to cache already decrypted key
	loadingCache sync.Map // map[string][]byte

	encryptedDataKeyMap sync.Map // map[string]EncryptionKeyInfo

	logCtx string

	logger log.Logger

	cipherLock sync.Mutex

	encryptLock sync.Mutex
}

func NewDefaultMessageCrypto(logCtx string, keyGenNeeded bool, logger log.Logger) (*DefaultMessageCrypto, error) {

	d := &DefaultMessageCrypto{
		logCtx:              logCtx,
		loadingCache:        sync.Map{},
		encryptedDataKeyMap: sync.Map{},
		logger:              logger,
	}

	if keyGenNeeded {
		key, err := generateDataKey()
		if err != nil {
			return d, err
		}
		d.dataKey = key
	}

	return d, nil
}

func (d *DefaultMessageCrypto) AddPublicKeyCipher(keyNames []string, keyCrypto interface{}) error {
	key, err := generateDataKey()
	if err != nil {
		return err
	}

	d.dataKey = key
	for _, keyName := range keyNames {
		err := d.addPublicKeyCipher(keyName, keyCrypto)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DefaultMessageCrypto) addPublicKeyCipher(keyName string, keyCrypto interface{}) error {
	d.cipherLock.Lock()
	defer d.cipherLock.Unlock()
	if keyName != "" && keyCrypto != nil {

		// Use CryptoKeyReader to get public key & encrypt data key using it
		cryptoKeyReader, ok := keyCrypto.(CryptoKeyReader)
		if !ok {
			// Use DataKeyCrypto to encrypt data key
			datakeyCrypto, ok := keyCrypto.(DataKeyCrypto)
			if !ok {
				return fmt.Errorf("invalid cryptokeyreader")
			}
			key, err := datakeyCrypto.EncryptDataKey(d.dataKey)
			if err != nil {
				return err
			}

			d.encryptedDataKeyMap.Store(keyName, NewEncryptionKeyInfo(key, nil))
			return nil
		}

		// else load the private key and encrypt the datakey using it
		cryptoKeyReader.getPublicKey(keyName, nil)
		// TODO Complete the remaining functionality
	}
	return nil
}

func (d *DefaultMessageCrypto) RemoveKeyCipher(keyName string) bool {
	if keyName == "" {
		return false
	}
	d.encryptedDataKeyMap.Delete(keyName)
	return true
}

func (d *DefaultMessageCrypto) encrypt(encKeys []string, keyCrypto interface{}, msgMetadata *pb.MessageMetadata, payload []byte) ([]byte, error) {
	d.encryptLock.Lock()
	defer d.encryptLock.Unlock()
	if len(encKeys) == 0 {
		return payload, nil
	}

	// update message metadata with encrypted data key
	encryptionKeys := msgMetadata.GetEncryptionKeys()

	for _, keyName := range encKeys {
		// if key is not already loaded, load it
		if _, ok := d.encryptedDataKeyMap.Load(keyName); !ok {
			d.addPublicKeyCipher(keyName, keyCrypto)
		}

		// add key to the message metadata
		if k, ok := d.encryptedDataKeyMap.Load(keyName); ok {
			keyInfo, keyInfoOk := k.(*EncryptionKeyInfo)

			if keyInfoOk {

				newEncryptionKey := &pb.EncryptionKeys{
					Key:   &keyName,
					Value: keyInfo.GetKey(),
				}

				if keyInfo.metadata != nil && len(keyInfo.metadata) > 0 {
					keyMetadata := newEncryptionKey.GetMetadata()
					for k, v := range keyInfo.GetMetadata() {
						keyMetadata = append(keyMetadata, &pb.KeyValue{
							Key:   &k,
							Value: &v,
						})
					}
					newEncryptionKey.Metadata = keyMetadata
				}

				encryptionKeys = append(encryptionKeys, newEncryptionKey)

			} else {
				d.logger.Error("Failed to get EncryptionKeyInfo for key %s", keyName)
			}
		} else {
			// we should never reach here
			d.logger.Errorf("%s Failed to find encrypted Data key for key %s", d.logCtx, keyName)
		}

	}

	// update message metadata with EncryptionKeys
	msgMetadata.EncryptionKeys = encryptionKeys

	// generate a new AES cipher with data key
	c, err := aes.NewCipher(d.dataKey)

	if err != nil {
		d.logger.Error("Failed to create AES cipher")
		return nil, err
	}

	// gcm
	gcm, err := cipher.NewGCM(c)

	if err != nil {
		d.logger.Error("Failed to create gcm")
		return nil, err
	}

	// create gcm param
	nonce := make([]byte, gcm.NonceSize())
	_, err = rand.Read(nonce)

	if err != nil {
		d.logger.Error("Failed to generate new nonce ")
	}

	// Update message metadata with encryption param
	msgMetadata.EncryptionParam = nonce

	// encrypt payload using seal function
	return gcm.Seal(nil, nonce, payload, nil), nil
}

func (d *DefaultMessageCrypto) Encrypt(encKeys []string, cryptoKeyReader CryptoKeyReader, msgMetadata *pb.MessageMetadata, payload []byte) ([]byte, error) {
	return d.encrypt(encKeys, cryptoKeyReader, msgMetadata, payload)
}

func (d *DefaultMessageCrypto) EncryptWithDataKeyCrypto(encKeys []string, dataKeyCrypto DataKeyCrypto, msgMetadata *pb.MessageMetadata, payload []byte) ([]byte, error) {
	return d.encrypt(encKeys, dataKeyCrypto, msgMetadata, payload)
}

func (d *DefaultMessageCrypto) Decrypt(msgMetadata *pb.MessageMetadata, payload []byte, cryptoKeyReader CryptoKeyReader) ([]byte, error) {
	return d.decrypt(msgMetadata, payload, cryptoKeyReader)
}

func (d *DefaultMessageCrypto) DecryptWithDataKeyCrypto(msgMetadata *pb.MessageMetadata, payload []byte, dataKeyCrypto DataKeyCrypto) ([]byte, error) {
	return d.decrypt(msgMetadata, payload, dataKeyCrypto)
}

func (d *DefaultMessageCrypto) decrypt(msgMetadata *pb.MessageMetadata, payload []byte, keyReader interface{}) ([]byte, error) {
	// if data key is present, attempt to derypt using the existing key
	if d.dataKey != nil {
		decryptedData, err := d.getKeyAndDecryptData(msgMetadata, payload)
		if err != nil {
			d.logger.Error(err)
		}

		if decryptedData != nil {
			return decryptedData, nil
		}
	}

	// data key is null or decryption failed. Attempt to regenerate data key
	encKeys := msgMetadata.GetEncryptionKeys()
	// decrypt data keys
	encryptionKeys := []pb.EncryptionKeys{}

	for _, key := range encKeys {
		if d.decryptDataKey(key.GetKey(), key.GetValue(), key.GetMetadata(), keyReader) {
			encryptionKeys = append(encryptionKeys, *key)
		}
	}

	if len(encryptionKeys) == 0 {
		return nil, errors.New("unable to decrypt data key")
	}

	return d.getKeyAndDecryptData(msgMetadata, payload)
}

func (d *DefaultMessageCrypto) decryptData(dataKeySecret []byte, msgMetadata *pb.MessageMetadata, payload []byte) ([]byte, error) {
	// get nonce from message metadata
	nonce := msgMetadata.GetEncryptionParam()

	c, err := aes.NewCipher(dataKeySecret)

	if err != nil {
		d.logger.Error(err)
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	decryptedData, err := gcm.Open(nil, nonce, payload, nil)

	if err != nil {
		d.logger.Error(err)
	}

	return decryptedData, err
}

func (d *DefaultMessageCrypto) getKeyAndDecryptData(msgMetadata *pb.MessageMetadata, payload []byte) ([]byte, error) {
	// go through all keys to retrieve data key from cache
	for _, k := range msgMetadata.GetEncryptionKeys() {
		msgDataKey := k.GetValue()
		keyDigest := fmt.Sprintf("%x", md5.Sum(msgDataKey))
		if storedSecretKey, ok := d.loadingCache.Load(keyDigest); ok {
			decryptedData, err := d.decryptData(storedSecretKey.([]byte), msgMetadata, payload)
			if err != nil {
				d.logger.Error(err)
			}

			if decryptedData != nil {
				return decryptedData, nil
			}
		} else {
			// First time, entry wont be present in cache
			d.logger.Debugf("%s Failed to decrypt data or data key is not in cache. Will attempt to refresh", d.logCtx)
		}
	}
	return nil, nil
}

func (d *DefaultMessageCrypto) decryptDataKey(keyName string, encDatakey []byte, encKeymeta []*pb.KeyValue, keyCrypto interface{}) bool {
	keyMeta := make(map[string]string)
	for _, k := range encKeymeta {
		keyMeta[k.GetKey()] = k.GetValue()
	}

	keyReader, keyReaderOk := keyCrypto.(CryptoKeyReader)
	if !keyReaderOk {
		datakeyCrypto, datakeyCryptoOk := keyCrypto.(DataKeyCrypto)
		if datakeyCryptoOk {
			decryptedDatakey, err := datakeyCrypto.DecryptDataKey(encDatakey)
			if err != nil {
				d.logger.Error(err)
				return false
			}
			d.dataKey = decryptedDatakey
			d.loadingCache.Store(fmt.Sprintf("%x", md5.Sum(encDatakey)), d.dataKey)
		} else {
			d.logger.Error("Invalid keycrypto parameter")
			return false
		}
	} else {
		// TODO complete data key decrypt using private key
		keyReader.getPrivateKey(keyName, keyMeta)
	}
	return true
}

func generateDataKey() ([]byte, error) {
	key := make([]byte, 32)  // generate key of length 256 bytes
	_, err := rand.Read(key) // cryptographically secure random number
	return key, err
}
