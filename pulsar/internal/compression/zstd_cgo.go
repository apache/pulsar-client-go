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

// +build cgo

// If CGO is enabled, use ZSTD library that links with official
// C based zstd. This can perform both compression and decompression.

package compression

import (
	zstd "github.com/valyala/gozstd"
)

func NewZStdProvider() Provider {
	return newCGoZStdProvider()
}

type zstdProvider struct {
}

func newCGoZStdProvider() Provider {
	return &zstdProvider{}
}

func (zstdProvider) CanCompress() bool {
	return true
}

func (zstdProvider) Compress(data []byte) []byte {
	return zstd.Compress(nil, data)
}

func (zstdProvider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	return zstd.Decompress(nil, compressedData)
}
