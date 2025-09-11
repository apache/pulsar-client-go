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
	case <-ch:
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
	case <-ch:
		assert.Fail(t, "Should not have had a value at this point")
	default:
		// Good, no value yet
	}

	assert.Equal(t, "test-1", q.Poll())

	// Now the go-routine should have completed
	<-ch
	assert.Equal(t, 3, q.Size())

	assert.Equal(t, "test-2", q.Take())
	assert.Equal(t, "test-3", q.Take())
	assert.Equal(t, "test-4", q.Take())

	close(ch)
}

func TestBlockingQueue_ReadableSlice(t *testing.T) {
	q := NewBlockingQueue(3)

	q.Put(1)
	q.Put(2)
	q.Put(3)
	assert.Equal(t, 3, q.Size())

	items := q.ReadableSlice()
	assert.Equal(t, len(items), 3)
	assert.Equal(t, items[0], 1)
	assert.Equal(t, items[1], 2)
	assert.Equal(t, items[2], 3)

	q.Poll()

	items = q.ReadableSlice()
	assert.Equal(t, len(items), 2)
	assert.Equal(t, items[0], 2)
	assert.Equal(t, items[1], 3)

	q.Put(4)

	items = q.ReadableSlice()
	assert.Equal(t, len(items), 3)
	assert.Equal(t, items[0], 2)
	assert.Equal(t, items[1], 3)
	assert.Equal(t, items[2], 4)
}

func TestBlockingQueueIterate(t *testing.T) {
	bq := NewBlockingQueue(5)

	// Add some items
	bq.Put("item1")
	bq.Put("item2")
	bq.Put("item3")

	// Test iteration
	items := make([]interface{}, 0)
	bq.Iterate(func(item interface{}) {
		items = append(items, item)
	})

	assert.Equal(t, 3, len(items))
	assert.Equal(t, "item1", items[0])
	assert.Equal(t, "item2", items[1])
	assert.Equal(t, "item3", items[2])
}

func TestBlockingQueueIteratePartial(t *testing.T) {
	bq := NewBlockingQueue(5)

	// Add some items
	bq.Put("item1")
	bq.Put("item2")
	bq.Put("item3")

	// Test partial iteration (first 2 items only)
	items := make([]interface{}, 0)
	bq.Iterate(func(item interface{}) {
		if len(items) < 2 {
			items = append(items, item)
		}
	})

	assert.Equal(t, 2, len(items))
	assert.Equal(t, "item1", items[0])
	assert.Equal(t, "item2", items[1])
}

func TestBlockingQueueIterateCircularBuffer(t *testing.T) {
	bq := NewBlockingQueue(3)

	// Fill the queue to test circular buffer behavior
	bq.Put("item1")
	bq.Put("item2")
	bq.Put("item3")

	// Remove one item to create space
	bq.Poll()

	// Add another item to test wrapping
	bq.Put("item4")

	// Test iteration with circular buffer
	items := make([]interface{}, 0)
	bq.Iterate(func(item interface{}) {
		items = append(items, item)
	})

	assert.Equal(t, 3, len(items))
	assert.Equal(t, "item2", items[0])
	assert.Equal(t, "item3", items[1])
	assert.Equal(t, "item4", items[2])
}

func TestBlockingQueueIterateEmpty(t *testing.T) {
	bq := NewBlockingQueue(5)

	// Test iteration on empty queue
	items := make([]interface{}, 0)
	bq.Iterate(func(item interface{}) {
		items = append(items, item)
	})

	assert.Equal(t, 0, len(items))
}
