package crypto

type ProducerCryptoFailureAction int
type ConsumerCryptoFailureAction int

const (
	// FAIL_SEND this is the default option to fail send if crypto operation fails.
	FAIL_SEND ProducerCryptoFailureAction = iota

	// SEND ingnore crypto failure and proceed with sending unencrypted message.
	SEND
)

const (
	// FAIL this is the default option to fail consume messages until crypto succeeds.
	FAIL_CONSUME ConsumerCryptoFailureAction = iota

	// DISCARD  message is silently acknowledged and not delivered to the application
	DISCARD

	// CONSUME deliver the encrypted message to the application. It's the application's responsibility to decrypt the message.
	// if message is also compressed, decompression will fail. If message contain batch messages, client will not be able to retrieve individual messages in the batch.
	// delivered encrypted message contains EncryptionContext which contains encryption and compression information in it using which application can decrypt consumed message payload.
	CONSUME
)
