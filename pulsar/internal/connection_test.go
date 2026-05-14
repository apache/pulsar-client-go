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

package internal

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
)

func TestConnectionWriteDataShouldNotEnqueueWhenStateClosed(t *testing.T) {
	released := atomic.Int64{}
	pool := NewBufferPool()
	buf := pool.GetBuffer(8)
	buf.SetReleaseCallback(func() {
		released.Add(1)
	})

	c := &connection{
		log:             log.DefaultNopLogger(),
		closeCh:         make(chan struct{}),
		writeRequestsCh: make(chan *dataRequest, 1),
	}
	c.setStateClosed()

	c.WriteData(context.Background(), buf)

	assert.Equal(t, 0, len(c.writeRequestsCh))
	assert.EqualValues(t, 1, released.Load())
}
