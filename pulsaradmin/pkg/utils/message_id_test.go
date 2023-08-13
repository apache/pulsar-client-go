// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseMessageId(t *testing.T) {
	id, err := ParseMessageID("1:1")
	assert.Nil(t, err)
	assert.Equal(t, MessageID{LedgerID: 1, EntryID: 1, PartitionIndex: -1, BatchIndex: -1}, *id)

	id, err = ParseMessageID("1:2:3")
	assert.Nil(t, err)
	assert.Equal(t, MessageID{LedgerID: 1, EntryID: 2, PartitionIndex: 3, BatchIndex: -1}, *id)

	id, err = ParseMessageID("1:2:3:4")
	assert.Nil(t, err)
	assert.Equal(t, MessageID{LedgerID: 1, EntryID: 2, PartitionIndex: 3, BatchIndex: 4}, *id)
}

func TestParseMessageIdErrors(t *testing.T) {
	id, err := ParseMessageID("1;1")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid message id string. 1;1", err.Error())

	id, err = ParseMessageID("a:1")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid ledger id. a:1", err.Error())

	id, err = ParseMessageID("1:a")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid entry id. 1:a", err.Error())

	id, err = ParseMessageID("1:2:a")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid partition index. 1:2:a", err.Error())

	id, err = ParseMessageID("1:2:3:a")
	assert.Nil(t, id)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid batch index. 1:2:3:a", err.Error())
}
