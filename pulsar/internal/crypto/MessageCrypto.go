package crypto

import (
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

// MessageCrypto implement this interface to encrypt and decrypt messages
type MessageCrypto interface {
	// AddPublicKeyCipher
	// @param keyNames
	// @keyCrypto it can be either CryptoKeyReader or DataKeyCrypto
	AddPublicKeyCipher(keyNames []string, keyCrypto interface{}) error

	// RemoveKeyCipher remove the key indentified by the keyname from the list
	RemoveKeyCipher(keyName string) bool

	/*
		Encrypt the payload using the data key and update message metadata with the keyname and encrypted data key
	*/
	Encrypt(encKeys []string, cryptoKeyReader CryptoKeyReader, msgMetadata *pb.MessageMetadata, payload []byte) ([]byte, error)

	/*
		EncryptWithDataKeyCrypto similar to above but client needs to handle encryption of data key
	*/
	EncryptWithDataKeyCrypto(encKeys []string, dataKeyCrypto DataKeyCrypto, msgMetadata *pb.MessageMetadata, payload []byte) ([]byte, error)

	/*
		Decrypt the payload using the data key. Keys used to ecnrypt the data key can be retrived from msgMetadata
	*/
	Decrypt(msgMetadata *pb.MessageMetadata, payload []byte, cryptoKeyReader CryptoKeyReader) ([]byte, error)

	/*
		DecryptWithDataKeyCrypto similar to above method but client needs to hanndle decryption of data key
	*/
	DecryptWithDataKeyCrypto(msgMetadata *pb.MessageMetadata, payload []byte, cryptoKeyReader CryptoKeyReader) ([]byte, error)
}
