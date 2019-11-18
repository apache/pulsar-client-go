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
	"bytes"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

type zstdProvider struct {
	encoder *zstd.Encoder
}

func NewZStdProvider() Provider {
	p := &zstdProvider{}
	p.encoder, _ = zstd.NewWriter(nil)
	return p
}

func (p *zstdProvider) CanCompress() bool {
	return true
}

func (p *zstdProvider) Compress(data []byte) []byte {
	return p.encoder.EncodeAll(data, []byte{})
}

func (p* zstdProvider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	d, err := zstd.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, err
	}

	uncompressed := make([]byte, originalSize)
	size, err := d.Read(uncompressed)
	if err != nil {
		return nil, err
	} else if size != originalSize {
		return nil, errors.New("Invalid uncompressed size")
	} else {
		return uncompressed, nil
	}
}
