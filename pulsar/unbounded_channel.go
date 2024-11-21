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
	"container/list"
)

type unboundedChannel[T interface{}] struct {
	values  *list.List
	inCh    chan<- T
	outCh   <-chan T
	closeCh chan struct{}
}

func newUnboundedChannel[T interface{}]() *unboundedChannel[T] {
	inCh := make(chan T)
	outCh := make(chan T)
	c := &unboundedChannel[T]{
		values:  list.New(),
		inCh:    inCh,
		outCh:   outCh,
		closeCh: make(chan struct{}),
	}
	go func() {
		for {
			front := c.values.Front()
			var ch chan T
			var value T
			if front != nil {
				value = front.Value.(T)
				ch = outCh
			}
			// A send to a nil channel blocks forever so when no values are available,
			// it would never send a value to ch
			select {
			case v := <-inCh:
				c.values.PushBack(v)
			case ch <- value:
				c.values.Remove(front)
			case <-c.closeCh:
				close(inCh)
				close(outCh)
				return
			}
		}
	}()
	return c
}

func (c *unboundedChannel[T]) stop() {
	c.closeCh <- struct{}{}
}
