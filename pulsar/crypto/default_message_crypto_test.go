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
	"testing"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
)

func TestAddPublicKeyCipher(t *testing.T) {
	msgCrypto, err := NewDefaultMessageCrypto("test-default-crypto", true, log.DefaultNopLogger())
	assert.Nil(t, err)
	assert.NotNil(t, msgCrypto)

	// valid keyreader
	err = msgCrypto.AddPublicKeyCipher(
		[]string{"my-app.key"},
		NewFileKeyReader("../crypto/testdata/pub_key_rsa.pem", ""),
	)
	assert.Nil(t, err)

	// invalid keyreader
	err = msgCrypto.AddPublicKeyCipher(
		[]string{"my-app0.key"},
		NewFileKeyReader("../crypto/testdata/no_pub_key_rsa.pem", ""),
	)
	assert.NotNil(t, err)

	// empty keyreader
	err = msgCrypto.AddPublicKeyCipher(
		[]string{"my-app1.key"},
		nil,
	)
	assert.NotNil(t, err)

	// keyreader with wrong econding of public key
	err = msgCrypto.AddPublicKeyCipher(
		[]string{"my-app2.key"},
		NewFileKeyReader("../crypto/testdata/wrong_encode_pub_key_rsa.pem", ""),
	)
	assert.NotNil(t, err)

	// keyreader with truncated pub key
	err = msgCrypto.AddPublicKeyCipher(
		[]string{"my-app2.key"},
		NewFileKeyReader("../crypto/testdata/truncated_pub_key_rsa.pem", ""),
	)
	assert.NotNil(t, err)
}

func TestEncrypt(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}
	msgMetadataSupplier := NewMessageMetadataSupplier(msgMetadata)

	msg := "my-message-01"

	msgCrypto, err := NewDefaultMessageCrypto("my-app", true, log.DefaultNopLogger())
	assert.Nil(t, err)
	assert.NotNil(t, msgCrypto)

	// valid keyreader
	encryptedData, err := msgCrypto.Encrypt(
		[]string{"my-app.key"},
		NewFileKeyReader("../crypto/testdata/pub_key_rsa.pem", ""),
		msgMetadataSupplier,
		[]byte(msg),
	)

	assert.Nil(t, err)
	assert.NotNil(t, encryptedData)

	// encrypted data key and encryption param must set in
	// in the message metadata after encryption
	assert.NotNil(t, msgMetadataSupplier.EncryptionParam())
	assert.NotEmpty(t, msgMetadataSupplier.EncryptionKeys())

	// invalid keyreader
	encryptedData, err = msgCrypto.Encrypt(
		[]string{"my-app2.key"},
		NewFileKeyReader("../crypto/testdata/no_pub_key_rsa.pem", ""),
		msgMetadataSupplier,
		[]byte(msg),
	)

	assert.NotNil(t, err)
	assert.Nil(t, encryptedData)
}

func TestEncryptDecrypt(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}
	msgMetadataSupplier := NewMessageMetadataSupplier(msgMetadata)

	msg := "my-message-01"

	msgCrypto, err := NewDefaultMessageCrypto("my-app", true, log.DefaultNopLogger())
	assert.Nil(t, err)
	assert.NotNil(t, msgCrypto)

	// valid keyreader
	encryptedData, err := msgCrypto.Encrypt(
		[]string{"my-app.key"},
		NewFileKeyReader("../crypto/testdata/pub_key_rsa.pem", ""),
		msgMetadataSupplier,
		[]byte(msg),
	)

	assert.Nil(t, err)
	assert.NotNil(t, encryptedData)

	// encrypted data key and encryption param must set in
	// in the message metadata after encryption
	assert.NotNil(t, msgMetadataSupplier.EncryptionParam())
	assert.NotEmpty(t, msgMetadataSupplier.EncryptionKeys())

	// try to decrypt
	msgCryptoDecrypt, err := NewDefaultMessageCrypto("my-app", true, log.DefaultNopLogger())
	assert.Nil(t, err)
	assert.NotNil(t, msgCrypto)

	// keyreader with invalid private key
	decryptedData, err := msgCryptoDecrypt.Decrypt(
		msgMetadataSupplier,
		encryptedData,
		NewFileKeyReader("", "../crypto/testdata/no_pri_key_rsa.pem"),
	)
	assert.NotNil(t, err)
	assert.Nil(t, decryptedData)

	// keyreader with wrong encoded private key
	decryptedData, err = msgCryptoDecrypt.Decrypt(
		msgMetadataSupplier,
		encryptedData,
		NewFileKeyReader("", "../crypto/testdata/wrong_encoded_pri_key_rsa.pem"),
	)
	assert.NotNil(t, err)
	assert.Nil(t, decryptedData)

	// keyreader with valid private key
	decryptedData, err = msgCryptoDecrypt.Decrypt(
		msgMetadataSupplier,
		encryptedData,
		NewFileKeyReader("", "../crypto/testdata/pri_key_rsa.pem"),
	)

	assert.Nil(t, err)
	assert.Equal(t, msg, string(decryptedData))
}
