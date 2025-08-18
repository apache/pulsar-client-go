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
	"github.com/klauspost/compress/snappy"
)

type snappyProvider struct {
}

// NewSnappyProvider returns a Provider interface
func NewSnappyProvider() Provider {
	return &snappyProvider{}
}

func (p *snappyProvider) CompressMaxSize(srcSize int) int {
	return snappy.MaxEncodedLen(srcSize)
}

func (p *snappyProvider) Compress(dst, src []byte) []byte {
	return snappy.Encode(dst, src)
}

func (p *snappyProvider) Decompress(dst, src []byte, _ int) ([]byte, error) {
	return snappy.Decode(dst, src)
}

func (p *snappyProvider) Close() error {
	return nil
}

func (p *snappyProvider) Clone() Provider {
	return &snappyProvider{}
}
