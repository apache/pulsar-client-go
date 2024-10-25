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

//go:build cgo
// +build cgo

// If CGO is enabled, use ZSTD library that links with official
// C based zstd which provides better performance compared with
// respect to the native Go implementation of ZStd.

package compression

import (
	"sync"

	"github.com/DataDog/zstd"
	log "github.com/sirupsen/logrus"
)

type zstdCGoProvider struct {
	ctxPool   sync.Pool
	level     Level
	zstdLevel int
}

func newCGoZStdProvider(level Level) Provider {
	z := &zstdCGoProvider{
		ctxPool: sync.Pool{New: func() any {
			return zstd.NewCtx()
		}},
	}

	switch level {
	case Default:
		z.zstdLevel = zstd.DefaultCompression
	case Faster:
		z.zstdLevel = zstd.BestSpeed
	case Better:
		z.zstdLevel = 9
	}

	return z
}

func NewZStdProvider(level Level) Provider {
	return newCGoZStdProvider(level)
}

func (z *zstdCGoProvider) CompressMaxSize(originalSize int) int {
	return zstd.CompressBound(originalSize)
}

func (z *zstdCGoProvider) Compress(dst, src []byte) []byte {
	ctx := z.ctxPool.Get().(zstd.Ctx)
	defer z.ctxPool.Put(ctx)
	out, err := ctx.CompressLevel(dst, src, z.zstdLevel)
	if err != nil {
		log.WithError(err).Fatal("Failed to compress")
	}

	return out
}

func (z *zstdCGoProvider) Decompress(dst, src []byte, _ int) ([]byte, error) {
	ctx := z.ctxPool.Get().(zstd.Ctx)
	defer z.ctxPool.Put(ctx)
	return ctx.Decompress(dst, src)
}

func (z *zstdCGoProvider) Close() error {
	return nil
}

func (z *zstdCGoProvider) Clone() Provider {
	return newCGoZStdProvider(z.level)
}
