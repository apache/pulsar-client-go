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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageId(t *testing.T) {
	id := newMessageID(1, 2, 3, 4)
	bytes := id.Serialize()

	id2, err := DeserializeMessageID(bytes)
	assert.NoError(t, err)
	assert.NotNil(t, id2)

	assert.Equal(t, int64(1), id2.(*messageID).ledgerID)
	assert.Equal(t, int64(2), id2.(*messageID).entryID)
	assert.Equal(t, 3, id2.(*messageID).batchIdx)
	assert.Equal(t, 4, id2.(*messageID).partitionIdx)

	id, err = DeserializeMessageID(nil)
	assert.Error(t, err)
	assert.Nil(t, id)

	id, err = DeserializeMessageID(make([]byte, 0))
	assert.Error(t, err)
	assert.Nil(t, id)
}
