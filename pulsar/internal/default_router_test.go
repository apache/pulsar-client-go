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

package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultRouter(t *testing.T) {

	var currentClock uint64

	router := NewDefaultRouter(func() uint64 {
		return currentClock
	}, JavaStringHash, 10*time.Nanosecond)

	// partition index should not change with time
	p1 := router("my-key", 100)
	p2 := router("my-key", 100)

	assert.Equal(t, p1, p2)

	currentClock = 100
	p3 := router("my-key", 100)

	assert.Equal(t, p1, p3)

	// With no key, we should give the same partition for a given time range
	pr1 := router("", 100)
	pr2 := router("", 100)
	assert.Equal(t, pr1, pr2)

	currentClock = 101
	pr3 := router("", 100)
	assert.Equal(t, pr1, pr3)

	currentClock = 102
	pr4 := router("", 100)
	assert.Equal(t, pr1, pr4)

	currentClock = 111
	pr5 := router("", 100)
	assert.NotEqual(t, pr1, pr5)

	currentClock = 112
	pr6 := router("", 100)
	assert.Equal(t, pr5, pr6)
}

func TestDefaultRouterNoPartitions(t *testing.T) {

	router := NewDefaultRouter(NewSystemClock(), JavaStringHash, 10*time.Nanosecond)

	// partition index should not change with time
	p1 := router("", 1)
	p2 := router("my-key", 1)
	p3 := router("my-key-2", 1)
	p4 := router("my-key-3", 1)

	assert.Equal(t, 0, p1)
	assert.Equal(t, 0, p2)
	assert.Equal(t, 0, p3)
	assert.Equal(t, 0, p4)
}
