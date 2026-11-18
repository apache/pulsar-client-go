// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	internalcrypto "github.com/apache/pulsar-client-go/pulsar/internal/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

// TestEncryptedBatchMessageWithoutUncompressedSize verifies that an
// encrypted, uncompressed batch message is consumed correctly when the
// producer left UncompressedSize unset in the message metadata.
//
// The C++ client (and the Python client built on it) only sets
// uncompressed_size when compression is enabled, so an encrypted batch
// produced with CompressionNone arrives without the field. The consumer
// then skipped both the decompression and the reader reset, leaving the
// reader positioned over the ciphertext of the wire payload instead of
// the decrypted batch payload.
// See https://github.com/apache/pulsar-client-go/issues/1527
func TestEncryptedBatchMessageWithoutUncompressedSize(t *testing.T) {
	keyReader := crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
		"crypto/testdata/pri_key_rsa.pem")

	producerMessageCrypto, err := crypto.NewDefaultMessageCrypto("encrypt", true, log.DefaultNopLogger())
	assert.Nil(t, err)
	encryptor := internalcrypto.NewProducerEncryptor(
		[]string{"client-rsa.pem"},
		keyReader,
		producerMessageCrypto,
		crypto.ProducerCryptoFailureActionFail,
		log.DefaultNopLogger())

	// Build the uncompressed batch payload: [smm size][smm][payload] per
	// message, the format shared by all clients.
	batch := internal.NewBuffer(512)
	values := []string{"msg-0", "msg-1"}
	for _, value := range values {
		smmBytes, err := proto.Marshal(&pb.SingleMessageMetadata{
			PayloadSize: proto.Int32(int32(len(value))),
		})
		assert.Nil(t, err)
		batch.WriteUint32(uint32(len(smmBytes)))
		batch.Write(smmBytes)
		batch.Write([]byte(value))
	}

	// Mirror the C++ producer with CompressionNone: the metadata carries
	// the encryption fields the encryptor sets, but no UncompressedSize.
	msgMeta := &pb.MessageMetadata{
		ProducerName:       proto.String("test-producer"),
		PublishTime:        proto.Uint64(1),
		SequenceId:         proto.Uint64(0),
		NumMessagesInBatch: proto.Int32(int32(len(values))),
	}
	encryptedPayload, err := encryptor.Encrypt(batch.ReadableSlice(), msgMeta)
	assert.Nil(t, err)

	metaBytes, err := proto.Marshal(msgMeta)
	assert.Nil(t, err)

	// Wire format: [MAGIC_NUMBER][CHECKSUM][METADATA_SIZE][METADATA][PAYLOAD]
	wire := internal.NewBuffer(1024)
	wire.WriteUint16(0x0e01) // magicCrc32c
	wire.WriteUint32(0)      // checksum placeholder
	metaStart := wire.WriterIndex()
	wire.WriteUint32(uint32(len(metaBytes)))
	wire.Write(metaBytes)
	wire.Write(encryptedPayload)
	checksum := internal.Crc32cCheckSum(wire.Get(metaStart, wire.WriterIndex()-metaStart))
	wire.PutUint32(checksum, 2)

	consumerMessageCrypto, err := crypto.NewDefaultMessageCrypto("decrypt", false, log.DefaultNopLogger())
	assert.Nil(t, err)
	pc := partitionConsumer{
		queueCh:              make(chan []*message, 1),
		eventsCh:             make(chan interface{}, 1),
		compressionProviders: sync.Map{},
		options:              &partitionConsumerOpts{},
		metrics:              newTestMetrics(),
		decryptor:            internalcrypto.NewConsumerDecryptor(keyReader, consumerMessageCrypto, log.DefaultNopLogger()),
		log:                  log.DefaultNopLogger(),
		client:               &client{rpcClient: &discardRPCClientStub{}},
	}
	pc._setConn(dummyConnection{})
	pc.availablePermits = &availablePermits{pc: &pc}
	pc.ackGroupingTracker = newAckGroupingTracker(&AckGroupingOptions{MaxSize: 0},
		func(id MessageID) { pc.sendIndividualAck(id) }, nil, nil)

	err = pc.MessageReceived(nil, internal.NewBufferWrapper(wire.Get(0, wire.WriterIndex())))
	assert.Nil(t, err)

	var messages []*message
	select {
	case messages = <-pc.queueCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for messages on queueCh")
	}
	assert.Len(t, messages, len(values))
	for i, msg := range messages {
		assert.Equal(t, []byte(values[i]), msg.Payload())
	}
}

// discardRPCClientStub satisfies internal.RPCClient for
// MessageReceived's corrupted-message path: only the no-wait ACK used to
// discard a message is ever sent, so the embedded interface is left nil
// and any other call would fail the test loudly.
type discardRPCClientStub struct {
	internal.RPCClient
}

func (s *discardRPCClientStub) NewRequestID() uint64 { return 1 }

func (s *discardRPCClientStub) RequestOnCnxNoWait(_ internal.Connection, _ pb.BaseCommand_Type,
	_ proto.Message) error {
	return nil
}
