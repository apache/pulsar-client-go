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

// EncryptionKeyInfo
type EncryptionKeyInfo struct {
	metadata map[string]string
	key      []byte
}

// NewEncryptionKeyInfo
func NewEncryptionKeyInfo(key []byte, metadata map[string]string) *EncryptionKeyInfo {
	return &EncryptionKeyInfo{
		metadata: metadata,
		key:      key,
	}
}

// GetKey get key
func (eci *EncryptionKeyInfo) GetKey() []byte {
	return eci.key
}

// SetKey set key
func (eci *EncryptionKeyInfo) SetKey(key []byte) {
	eci.key = key
}

// GetMetadata get key metadata
func (eci *EncryptionKeyInfo) GetMetadata() map[string]string {
	return eci.metadata
}

// SetMetadata set key metadata
func (eci *EncryptionKeyInfo) SetMetadata(metadata map[string]string) {
	eci.metadata = metadata
}
