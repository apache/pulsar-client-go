//
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
//

package compression

import (
	"github.com/cloudflare/golz4"
)

type lz4Provider struct {
}

func NewLz4Provider() Provider {
	return &lz4Provider{}
}

func (lz4Provider) Compress(data []byte) []byte {
	maxSize := lz4.CompressBound(data)
	compressed := make([]byte, maxSize)
	size, err := lz4.Compress(data, compressed)
	if err != nil {
		panic("Failed to compress")
	}
	return compressed[:size]
}

func (lz4Provider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	uncompressed := make([]byte, originalSize)
	err := lz4.Uncompress(compressedData, uncompressed)
	return uncompressed, err
}
