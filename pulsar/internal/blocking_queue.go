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
	"sync"

	log "github.com/sirupsen/logrus"
)

// BlockingQueue is a interface of block queue
type BlockingQueue interface {
	// Put enqueue one item, block if the queue is full
	Put(item interface{})

	// Take dequeue one item, block until it's available
	Take() interface{}

	// Poll dequeue one item, return nil if queue is empty
	Poll() interface{}

	// Poll dequeue one item if the item meets the condition,
	// return nil,true,false if queue is empty,
	// return nil,false,false if not satisfy.
	PollIfSatisfy(i func(item interface{}) bool) (item interface{}, empty bool, satisfy bool)

	// Peek return the first item without dequeing, return nil if queue is empty
	Peek() interface{}

	// PeekLast return last item in queue without dequeing, return nil if queue is empty
	PeekLast() interface{}

	// Size return the current size of the queue
	Size() int

	// if queue is not empty. iterate the whole queue and apply the function
	IterateIfNonEmpty(fun IterateFunc)
}

// if func return true then continue iterate blocking queue
type IterateFunc func(item interface{}) (continue_ bool)

// BlockingQueueIterator abstract a interface of block queue iterator.
type BlockingQueueIterator interface {
	HasNext() bool
	Next() interface{}
}

type blockingQueue struct {
	items   []interface{}
	headIdx int
	tailIdx int
	size    int
	maxSize int

	mutex      sync.Mutex
	isNotEmpty *sync.Cond
	isNotFull  *sync.Cond
}

type blockingQueueIterator struct {
	bq      *blockingQueue
	readIdx int
	toRead  int
}

// NewBlockingQueue init block queue and returns a BlockingQueue
func NewBlockingQueue(maxSize int) BlockingQueue {
	bq := &blockingQueue{
		items:   make([]interface{}, maxSize),
		headIdx: 0,
		tailIdx: 0,
		size:    0,
		maxSize: maxSize,
	}

	bq.isNotEmpty = sync.NewCond(&bq.mutex)
	bq.isNotFull = sync.NewCond(&bq.mutex)
	return bq
}

func (bq *blockingQueue) Put(item interface{}) {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	for bq.size == bq.maxSize {
		bq.isNotFull.Wait()
	}

	wasEmpty := bq.size == 0

	bq.items[bq.tailIdx] = item
	bq.size++
	bq.tailIdx++
	if bq.tailIdx >= bq.maxSize {
		bq.tailIdx = 0
	}

	if wasEmpty {
		// Wake up eventual reader waiting for next item
		bq.isNotEmpty.Signal()
	}
}

func (bq *blockingQueue) Take() interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	for bq.size == 0 {
		bq.isNotEmpty.Wait()
	}

	return bq.dequeue()
}

func (bq *blockingQueue) Poll() interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	if bq.size == 0 {
		return nil
	}

	return bq.dequeue()
}

func (bq *blockingQueue) PollIfSatisfy(condition func(interface{}) bool) (item interface{}, empty bool, satisfy bool) {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	if bq.size == 0 {
		return nil, true, false
	}

	if condition(bq.items[bq.headIdx]) {
		return bq.dequeue(), false, true
	}

	return nil, false, false
}

func (bq *blockingQueue) Peek() interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	if bq.size == 0 {
		return nil
	}
	return bq.items[bq.headIdx]
}

func (bq *blockingQueue) PeekLast() interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	if bq.size == 0 {
		return nil
	}
	idx := (bq.headIdx + bq.size - 1) % bq.maxSize
	return bq.items[idx]
}

func (bq *blockingQueue) dequeue() interface{} {
	item := bq.items[bq.headIdx]
	bq.items[bq.headIdx] = nil

	bq.headIdx++
	if bq.headIdx == len(bq.items) {
		bq.headIdx = 0
	}

	bq.size--
	bq.isNotFull.Signal()
	return item
}

func (bq *blockingQueue) Size() int {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	return bq.size
}

func (bq *blockingQueue) iterator() BlockingQueueIterator {
	return &blockingQueueIterator{
		bq:      bq,
		readIdx: bq.headIdx,
		toRead:  bq.size,
	}
}

func (bq *blockingQueue) IterateIfNonEmpty(operation IterateFunc) {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()
	if bq.size > 0 {
		for it := bq.iterator(); it.HasNext(); {
			if operation(it.Next()) {
				continue
			}
			break
		}
	}
}

func (bqi *blockingQueueIterator) HasNext() bool {
	return bqi.toRead > 0
}

func (bqi *blockingQueueIterator) Next() interface{} {
	if bqi.toRead == 0 {
		log.Panic("Trying to read past the end of the iterator")
	}

	item := bqi.bq.items[bqi.readIdx]
	bqi.toRead--
	bqi.readIdx++
	if bqi.readIdx == bqi.bq.maxSize {
		bqi.readIdx = 0
	}
	return item
}
