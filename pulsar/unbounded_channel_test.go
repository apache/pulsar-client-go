// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package pulsar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnboundedChannel(t *testing.T) {
	c := newUnboundedChannel[int]()
	defer c.stop()
	go func() {
		for i := 0; i < 10; i++ {
			c.inCh <- i
		}
	}()

	for i := 0; i < 10; i++ {
		v := <-c.outCh
		assert.Equal(t, i, v)
	}

	go func() {
		time.Sleep(1 * time.Second)
		c.inCh <- -1
	}()
	start := time.Now()
	v := <-c.outCh
	elapsed := time.Since(start)
	assert.Equal(t, v, -1)
	// Verify the read blocks for at least 500ms
	assert.True(t, elapsed >= 500*time.Millisecond)

	// Verify the send values will not be blocked
	for i := 0; i < 10000; i++ {
		c.inCh <- i
	}
	for i := 0; i < 10000; i++ {
		v := <-c.outCh
		assert.Equal(t, i, v)
	}
}
