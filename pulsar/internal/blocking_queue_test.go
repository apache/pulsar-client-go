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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBlockingQueue(t *testing.T) {
	q := NewBlockingQueue(10)

	assert.Equal(t, 0, q.Size())
	assert.Equal(t, nil, q.Poll())
	assert.Equal(t, nil, q.Peek())
	assert.Equal(t, nil, q.PeekLast())

	q.Put("test")
	assert.Equal(t, 1, q.Size())

	assert.Equal(t, "test", q.Peek())
	assert.Equal(t, "test", q.PeekLast())
	assert.Equal(t, 1, q.Size())

	assert.Equal(t, "test", q.Take())
	assert.Equal(t, nil, q.Peek())
	assert.Equal(t, nil, q.PeekLast())
	assert.Equal(t, 0, q.Size())

	ch := make(chan string)

	go func() {
		// Stays blocked until item is available
		ch <- q.Take().(string)
	}()

	time.Sleep(100 * time.Millisecond)

	select {
	case _ = <-ch:
		assert.Fail(t, "Should not have had a value at this point")
	default:
		// Good, no value yet
	}

	q.Put("test-2")

	x := <-ch
	assert.Equal(t, "test-2", x)

	// Fill the queue
	for i := 0; i < 10; i++ {
		q.Put(fmt.Sprintf("i-%d", i))
		assert.Equal(t, i+1, q.Size())
	}

	for i := 0; i < 10; i++ {
		assert.Equal(t, fmt.Sprintf("i-%d", i), q.Take())
	}

	close(ch)
}

func TestBlockingQueueWaitWhenFull(t *testing.T) {
	q := NewBlockingQueue(3)

	q.Put("test-1")
	q.Put("test-2")
	q.Put("test-3")
	assert.Equal(t, 3, q.Size())
	assert.Equal(t, "test-1", q.Peek())
	assert.Equal(t, "test-3", q.PeekLast())

	ch := make(chan bool)

	go func() {
		// Stays blocked until space is available
		q.Put("test-4")
		ch <- true
	}()

	time.Sleep(100 * time.Millisecond)

	select {
	case _ = <-ch:
		assert.Fail(t, "Should not have had a value at this point")
	default:
		// Good, no value yet
	}

	assert.Equal(t, "test-1", q.Poll())

	// Now the go-routine should have completed
	_ = <-ch
	assert.Equal(t, 3, q.Size())

	assert.Equal(t, "test-2", q.Take())
	assert.Equal(t, "test-3", q.Take())
	assert.Equal(t, "test-4", q.Take())

	close(ch)
}

func TestBlockingQueueIterator(t *testing.T) {
	q := NewBlockingQueue(10)

	q.Put(1)
	q.Put(2)
	q.Put(3)
	assert.Equal(t, 3, q.Size())

	i := 1
	for it := q.Iterator(); it.HasNext(); {
		assert.Equal(t, i, it.Next())
		i++
	}
}
