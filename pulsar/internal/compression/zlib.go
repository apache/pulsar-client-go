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
	"compress/zlib"
	"io"
)

type zlibProvider struct{}

// NewZLibProvider returns a Provider interface
func NewZLibProvider() Provider {
	return &zlibProvider{}
}

func (zlibProvider) CanCompress() bool {
	return true
}

func (zlibProvider) Compress(data []byte) []byte {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)

	if _, err := w.Write(data); err != nil {
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}

	return b.Bytes()
}

func (zlibProvider) Decompress(compressedData []byte, originalSize int) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, err
	}

	uncompressed := make([]byte, originalSize)
	if _, err = io.ReadFull(r, uncompressed); err != nil {
		return nil, err
	}

	if err = r.Close(); err != nil {
		return nil, err
	}

	return uncompressed, nil
}
