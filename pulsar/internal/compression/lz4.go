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
	"github.com/pierrec/lz4"
)

type lz4Provider struct {
}

func NewLz4Provider() Provider {
	return &lz4Provider{}
}

func (lz4Provider) Compress(data []byte) []byte {
	const tableSize = 1 << 16
	hashTable := make([]int, tableSize)

	maxSize := lz4.CompressBlockBound(len(data))
	compressed := make([]byte, maxSize)
	size, err := lz4.CompressBlock(data, compressed, hashTable)
	if err != nil {
		panic("Failed to compress")
	}

	if size == 0 {
		// The data block was not compressed. Just repeat it with
		// the block header flag to signal it's not compressed
		headerSize := writeSize(len(data), compressed)
		copy(compressed[headerSize:], data)
		return compressed[:len(data)+headerSize]
	} else {
		return compressed[:size]
	}
}

// Write the encoded size for the uncompressed payload
func writeSize(size int, dst []byte) int {
	if size < 0xF {
		dst[0] |= byte(size << 4)
		return 1
	} else {
		dst[0] |= 0xF0
		l := size - 0xF
		i := 1
		for ; l >= 0xFF; l -= 0xFF {
			dst[i] = 0xFF
			i++
		}
		dst[i] = byte(l)
		return i + 1
	}
}

func (lz4Provider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	uncompressed := make([]byte, originalSize)
	_, err := lz4.UncompressBlock(compressedData, uncompressed)
	return uncompressed, err
}
