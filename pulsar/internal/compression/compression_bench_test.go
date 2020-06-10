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

import (
	"io/ioutil"
	"testing"
)

var compressed int

func testCompression(b *testing.B, provider Provider) {
	data, err := ioutil.ReadFile("test_data_sample.txt")
	if err != nil {
		b.Error(err)
	}

	dataLen := int64(len(data))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Use len() to avoid the compiler optimizing the call away
		compressed = len(provider.Compress(data))
		b.SetBytes(dataLen)
	}
}

func testDecompression(b *testing.B, provider Provider) {
	// Read data sample file
	data, err := ioutil.ReadFile("test_data_sample.txt")
	if err != nil {
		b.Error(err)
	}

	dataCompressed := provider.Compress(data)

	dataLen := int64(len(data))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		provider.Decompress(dataCompressed, int(dataLen))
		b.SetBytes(dataLen)
	}
}

var benchmarkProviders = []testProvider{
	{"zlib", ZLibProvider, nil},
	{"lz4", Lz4Provider, nil},
	{"zstd-pure-go-fastest", newPureGoZStdProvider(1), nil},
	{"zstd-pure-go-default", newPureGoZStdProvider(2), nil},
	{"zstd-pure-go-best", newPureGoZStdProvider(3), nil},
	{"zstd-cgo-level-fastest", newCGoZStdProvider(1), nil},
	{"zstd-cgo-level-default", newCGoZStdProvider(3), nil},
	{"zstd-cgo-level-best", newCGoZStdProvider(9), nil},
}

func BenchmarkCompression(b *testing.B) {
	b.ReportAllocs()
	for _, provider := range benchmarkProviders {
		p := provider
		b.Run(p.name, func(b *testing.B) {
			testCompression(b, p.provider)
		})
	}
}

func BenchmarkDecompression(b *testing.B) {
	b.ReportAllocs()
	for _, provider := range benchmarkProviders {
		p := provider
		b.Run(p.name, func(b *testing.B) {
			testDecompression(b, p.provider)
		})
	}
}
