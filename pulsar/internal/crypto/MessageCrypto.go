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
	Encrypt(encKeys []string, keyCrypto interface{}, msgMetadata *pb.MessageMetadata, payload []byte) ([]byte, error)

	/*
		Decrypt the payload using the data key. Keys used to ecnrypt the data key can be retrived from msgMetadata
	*/
	Decrypt(msgMetadata *pb.MessageMetadata, payload []byte, keyCrypto interface{}) ([]byte, error)
}
