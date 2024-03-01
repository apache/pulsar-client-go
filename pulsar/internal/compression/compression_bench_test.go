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
	"os"
	"testing"
)

const (
	dataSampleFile = "test_data_sample.txt"
)

func testCompression(b *testing.B, provider Provider) {
	data, err := os.ReadFile(dataSampleFile)
	if err != nil {
		b.Error(err)
	}

	dataLen := int64(len(data))
	compressed := make([]byte, 1024*1024)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		provider.Compress(compressed[:0], data)
		b.SetBytes(dataLen)
	}
}

func testDecompression(b *testing.B, provider Provider) {
	// Read data sample file
	data, err := os.ReadFile(dataSampleFile)
	if err != nil {
		b.Error(err)
	}

	dataCompressed := provider.Compress(nil, data)
	dataDecompressed := make([]byte, 1024*1024)

	dataLen := int64(len(data))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		provider.Decompress(dataDecompressed[:0], dataCompressed, int(dataLen))
		b.SetBytes(dataLen)
	}
}

var benchmarkProviders = []testProvider{
	{"zlib", NewZLibProvider(), nil},
	{"lz4", NewLz4Provider(), nil},
	{"zstd-pure-go-fastest", newPureGoZStdProvider(Faster), nil},
	{"zstd-pure-go-default", newPureGoZStdProvider(Default), nil},
	{"zstd-pure-go-best", newPureGoZStdProvider(Better), nil},
	{"zstd-cgo-level-fastest", newCGoZStdProvider(Faster), nil},
	{"zstd-cgo-level-default", newCGoZStdProvider(Default), nil},
	{"zstd-cgo-level-best", newCGoZStdProvider(Better), nil},
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

func BenchmarkCompressionParallel(b *testing.B) {
	b.ReportAllocs()

	data, err := os.ReadFile(dataSampleFile)
	if err != nil {
		b.Error(err)
	}

	dataLen := int64(len(data))
	b.ResetTimer()

	for _, provider := range benchmarkProviders {
		p := provider
		b.Run(p.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				localProvider := p.provider.Clone()
				compressed := make([]byte, 1024*1024)

				for pb.Next() {
					localProvider.Compress(compressed[:0], data)
					b.SetBytes(dataLen)
				}
			})
		})
	}
}
