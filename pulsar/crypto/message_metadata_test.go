package crypto

import (
	"testing"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
)

func TestGetEncyptionKeys(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}
	key1 := "key-1"
	value1 := []byte{1, 2, 3, 4}

	key2 := "key-2"
	value2 := []byte{4, 3, 2, 1}

	key3 := "key-3"
	value3 := []byte{6, 7, 8, 9}

	msgMetadata.EncryptionKeys = append(msgMetadata.EncryptionKeys, &pb.EncryptionKeys{
		Key:   &key1,
		Value: value1,
		Metadata: []*pb.KeyValue{
			{Key: &key1, Value: &key1},
		},
	},
		&pb.EncryptionKeys{
			Key:   &key2,
			Value: value2,
			Metadata: []*pb.KeyValue{
				{Key: &key1, Value: &key1},
				{Key: &key2, Value: &key2},
			},
		},
		&pb.EncryptionKeys{
			Key:   &key3,
			Value: value3,
		},
	)

	expected := []EncryptionKeyInfo{
		{
			key:   key1,
			value: value1,
			metadata: map[string]string{
				"key-1": "key-1",
			},
		},
		{
			key:   key2,
			value: value2,
			metadata: map[string]string{
				"key-1": "key-1",
				"key-2": "key-2",
			},
		},
		{
			key:   key3,
			value: value3,
		},
	}

	msgMetadataSupplier := NewMessageMetadataSupplier(msgMetadata)
	actual := msgMetadataSupplier.GetEncryptionKeys()

	assert.EqualValues(t, expected, actual)
}

func TestUpsertEncryptionKey(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}
	key1 := "key-1"
	value1 := []byte{1, 2, 3, 4}

	keyInfo := NewEncryptionKeyInfo(key1, value1, map[string]string{"key-1": "value-1"})

	expected := []EncryptionKeyInfo{*keyInfo}

	msgMetadataSupplier := NewMessageMetadataSupplier(msgMetadata)

	msgMetadataSupplier.UpsertEncryptionkey(*keyInfo)

	// try to add same key again
	msgMetadataSupplier.UpsertEncryptionkey(*keyInfo)

	actual := msgMetadataSupplier.GetEncryptionKeys()

	assert.EqualValues(t, expected, actual)
}

func TestEncryptionParam(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}

	expected := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}

	msgMetadataSupplier := NewMessageMetadataSupplier(msgMetadata)
	msgMetadataSupplier.SetEncryptionParam(expected)

	assert.EqualValues(t, expected, msgMetadataSupplier.GetEncryptionParam())
}
