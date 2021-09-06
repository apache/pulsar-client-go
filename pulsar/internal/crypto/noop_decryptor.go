package crypto

import (
	"github.com/apache/pulsar-client-go/pulsar/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

type noopDecryptor struct{}

func NewNoopDecryptor() *noopDecryptor {
	return &noopDecryptor{}
}

// Decrypt noop decryptor
func (d *noopDecryptor) Decrypt(payload []byte, msgID *pb.MessageIdData, msgMetadata *pb.MessageMetadata) ([]byte, error) {
	return payload, nil
}

// CryptoFailureAction noop CryptoFailureAction
func (d *noopDecryptor) CryptoFailureAction() int {
	return crypto.ConsumerCryptoFailureActionFail
}
