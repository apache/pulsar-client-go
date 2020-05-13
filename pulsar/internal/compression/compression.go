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

package compression

// Provider is a interface of compression providers
type Provider interface {
	// CanCompress checks if the compression method is available under the current version.
	CanCompress() bool

	// Compress a []byte, the param is a []byte with the uncompressed content.
	// The reader/writer indexes will not be modified. The return is a []byte
	// with the compressed content.
	Compress(data []byte) []byte

	// Decompress a []byte. The buffer needs to have been compressed with the matching Encoder.
	// The compressedData is compressed content, originalSize is the size of the original content.
	// The return were the result will be passed, if err is nil, the buffer was decompressed, no nil otherwise.
	Decompress(compressedData []byte, originalSize int) ([]byte, error)
}

var (
	NoopProvider = NewNoopProvider
	ZLibProvider = NewZLibProvider
	Lz4Provider  = NewLz4Provider
	ZStdProvider = NewZStdProvider
)
