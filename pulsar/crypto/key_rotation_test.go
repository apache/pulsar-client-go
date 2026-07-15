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

package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync/atomic"
	"testing"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const keyIDMetadataKey = "key-id"

// rotatingKeyReader is a KeyReader that returns a different key-id metadata
// each time PublicKey is called, simulating key rotation in a key management
// service. The underlying RSA key bytes are the same (for simplicity), but the
// metadata changes — which is what matters for the rotation bug.
type rotatingKeyReader struct {
	pubKeyPEM  []byte
	privKeyPEM []byte
	callCount  atomic.Int32
	keyIDs     []string // keyIDs[callCount % len(keyIDs)] is returned
}

func newRotatingKeyReader(t *testing.T, keyIDs []string) *rotatingKeyReader {
	// Generate a test RSA key pair
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pubKeyBytes, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	require.NoError(t, err)
	pubKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubKeyBytes})

	privKeyBytes := x509.MarshalPKCS1PrivateKey(privKey)
	privKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privKeyBytes})

	return &rotatingKeyReader{
		pubKeyPEM:  pubKeyPEM,
		privKeyPEM: privKeyPEM,
		keyIDs:     keyIDs,
	}
}

func (r *rotatingKeyReader) PublicKey(keyName string, _ map[string]string) (*EncryptionKeyInfo, error) {
	idx := int(r.callCount.Add(1) - 1)
	keyID := r.keyIDs[idx%len(r.keyIDs)]
	return NewEncryptionKeyInfo(keyName, r.pubKeyPEM, map[string]string{keyIDMetadataKey: keyID}), nil
}

func (r *rotatingKeyReader) PrivateKey(keyName string, metadata map[string]string) (*EncryptionKeyInfo, error) {
	return NewEncryptionKeyInfo(keyName, r.privKeyPEM, metadata), nil
}

// CurrentKeyID returns what the NEXT PublicKey call would return (without incrementing).
func (r *rotatingKeyReader) CurrentKeyID() string {
	return r.keyIDs[int(r.callCount.Load())%len(r.keyIDs)]
}

// TestKeyRotationMetadataNotRefreshed demonstrates the bug: after the first
// Encrypt call for a given keyName, subsequent Encrypt calls reuse the cached
// encrypted data key and its metadata — even if the underlying key has rotated.
// This causes consumers to receive stale key-id metadata pointing at keys that
// may no longer exist, breaking decryption after key rotation.
//
// This test is expected to FAIL until the bug is fixed. When it fails, it proves
// the producer does not pick up key rotation.
func TestKeyRotationMetadataNotRefreshed(t *testing.T) {
	keyName := "latest"
	keyIDs := []string{"uuid-A", "uuid-B"}
	keyReader := newRotatingKeyReader(t, keyIDs)

	msgCrypto, err := NewDefaultMessageCrypto("test-rotation", true, log.DefaultNopLogger())
	require.NoError(t, err)

	// First encrypt: should capture uuid-A
	msg1Metadata := &pb.MessageMetadata{}
	msg1Supplier := NewMessageMetadataSupplier(msg1Metadata)
	_, err = msgCrypto.Encrypt([]string{keyName}, keyReader, msg1Supplier, []byte("message-1"))
	require.NoError(t, err)

	encKeys1 := msg1Supplier.EncryptionKeys()
	require.Len(t, encKeys1, 1)
	keyID1 := encKeys1[0].Metadata()[keyIDMetadataKey]
	assert.Equal(t, "uuid-A", keyID1, "first message should have uuid-A")

	// At this point, keyReader has been called once. The next call would return uuid-B.
	// Simulate rotation by noting that CurrentKeyID() is now uuid-B.
	assert.Equal(t, "uuid-B", keyReader.CurrentKeyID(), "after rotation, next key should be uuid-B")

	// Second encrypt: a correctly-behaving producer would re-invoke the KeyReader
	// and get uuid-B. The bug is that it reuses the cached entry from the first call.
	msg2Metadata := &pb.MessageMetadata{}
	msg2Supplier := NewMessageMetadataSupplier(msg2Metadata)
	_, err = msgCrypto.Encrypt([]string{keyName}, keyReader, msg2Supplier, []byte("message-2"))
	require.NoError(t, err)

	encKeys2 := msg2Supplier.EncryptionKeys()
	require.Len(t, encKeys2, 1)
	keyID2 := encKeys2[0].Metadata()[keyIDMetadataKey]

	// THIS ASSERTION WILL FAIL until the bug is fixed.
	// Current behavior: keyID2 == "uuid-A" (stale, cached from first encrypt)
	// Expected behavior: keyID2 == "uuid-B" (re-resolved after rotation)
	assert.Equal(t, "uuid-B", keyID2,
		"BUG: after key rotation, second message still has stale key-id metadata. "+
			"Expected uuid-B (rotated key), got %s (cached from first encrypt). "+
			"The producer never re-invokes KeyReader.PublicKey for an already-seen keyName, "+
			"so key rotation is invisible to messages produced after the first one.", keyID2)
}

// TestKeyRotationWorksWithRemoveKeyCipher shows the workaround: calling
// RemoveKeyCipher before encrypting forces a fresh KeyReader lookup.
// This demonstrates that the fix is to invalidate the cache on rotation.
func TestKeyRotationWorksWithRemoveKeyCipher(t *testing.T) {
	keyName := "latest"
	keyIDs := []string{"uuid-A", "uuid-B"}
	keyReader := newRotatingKeyReader(t, keyIDs)

	msgCrypto, err := NewDefaultMessageCrypto("test-rotation-workaround", true, log.DefaultNopLogger())
	require.NoError(t, err)

	// First encrypt: captures uuid-A
	msg1Metadata := &pb.MessageMetadata{}
	msg1Supplier := NewMessageMetadataSupplier(msg1Metadata)
	_, err = msgCrypto.Encrypt([]string{keyName}, keyReader, msg1Supplier, []byte("message-1"))
	require.NoError(t, err)

	encKeys1 := msg1Supplier.EncryptionKeys()
	require.Len(t, encKeys1, 1)
	assert.Equal(t, "uuid-A", encKeys1[0].Metadata()[keyIDMetadataKey])

	// Simulate rotation detected: remove the cached cipher so next encrypt re-resolves.
	// This is the workaround until a proper fix is implemented.
	removed := msgCrypto.RemoveKeyCipher(keyName)
	assert.True(t, removed, "RemoveKeyCipher should return true for existing key")

	// Second encrypt: now it WILL call PublicKey again and get uuid-B
	msg2Metadata := &pb.MessageMetadata{}
	msg2Supplier := NewMessageMetadataSupplier(msg2Metadata)
	_, err = msgCrypto.Encrypt([]string{keyName}, keyReader, msg2Supplier, []byte("message-2"))
	require.NoError(t, err)

	encKeys2 := msg2Supplier.EncryptionKeys()
	require.Len(t, encKeys2, 1)
	keyID2 := encKeys2[0].Metadata()[keyIDMetadataKey]

	// With RemoveKeyCipher workaround, this passes.
	assert.Equal(t, "uuid-B", keyID2,
		"With RemoveKeyCipher workaround, rotation is picked up. Got %s", keyID2)
}

// TestKeyRotationMultipleKeys verifies that when encrypting with multiple keys,
// rotation is detected independently per key.
func TestKeyRotationMultipleKeys(t *testing.T) {
	// Two keys: "latest" rotates, "stable" does not
	latestIDs := []string{"latest-A", "latest-B"}
	stableIDs := []string{"stable-A", "stable-A"} // same ID = no rotation

	latestReader := newRotatingKeyReader(t, latestIDs)
	stableReader := newRotatingKeyReader(t, stableIDs)

	// Combine into a multi-key reader
	multiReader := &multiKeyReader{
		readers: map[string]*rotatingKeyReader{
			"latest": latestReader,
			"stable": stableReader,
		},
	}

	msgCrypto, err := NewDefaultMessageCrypto("test-multi-key", true, log.DefaultNopLogger())
	require.NoError(t, err)

	// First encrypt with both keys
	msg1Metadata := &pb.MessageMetadata{}
	msg1Supplier := NewMessageMetadataSupplier(msg1Metadata)
	_, err = msgCrypto.Encrypt([]string{"latest", "stable"}, multiReader, msg1Supplier, []byte("message-1"))
	require.NoError(t, err)

	encKeys1 := msg1Supplier.EncryptionKeys()
	require.Len(t, encKeys1, 2)

	getKeyID := func(keys []EncryptionKeyInfo, name string) string {
		for _, k := range keys {
			if k.Name() == name {
				return k.Metadata()[keyIDMetadataKey]
			}
		}
		return ""
	}

	assert.Equal(t, "latest-A", getKeyID(encKeys1, "latest"))
	assert.Equal(t, "stable-A", getKeyID(encKeys1, "stable"))

	// Second encrypt: "latest" should rotate to B, "stable" stays A
	msg2Metadata := &pb.MessageMetadata{}
	msg2Supplier := NewMessageMetadataSupplier(msg2Metadata)
	_, err = msgCrypto.Encrypt([]string{"latest", "stable"}, multiReader, msg2Supplier, []byte("message-2"))
	require.NoError(t, err)

	encKeys2 := msg2Supplier.EncryptionKeys()
	require.Len(t, encKeys2, 2)

	assert.Equal(t, "latest-B", getKeyID(encKeys2, "latest"), "latest key should have rotated")
	assert.Equal(t, "stable-A", getKeyID(encKeys2, "stable"), "stable key should not have rotated")
}

// multiKeyReader delegates to different rotatingKeyReaders based on key name.
type multiKeyReader struct {
	readers map[string]*rotatingKeyReader
}

func (m *multiKeyReader) PublicKey(keyName string, meta map[string]string) (*EncryptionKeyInfo, error) {
	if r, ok := m.readers[keyName]; ok {
		return r.PublicKey(keyName, meta)
	}
	return nil, fmt.Errorf("unknown key: %s", keyName)
}

func (m *multiKeyReader) PrivateKey(keyName string, meta map[string]string) (*EncryptionKeyInfo, error) {
	if r, ok := m.readers[keyName]; ok {
		return r.PrivateKey(keyName, meta)
	}
	return nil, fmt.Errorf("unknown key: %s", keyName)
}
