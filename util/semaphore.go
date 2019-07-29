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

package util

// Semaphore is a channel of bool, used to receive a bool type semaphore.
type Semaphore chan bool

// Acquire a permit from this semaphore, blocking until one is available.

// Acquire a permit, if one is available and returns immediately,
// reducing the number of available permits by one.
func (s Semaphore) Acquire() {
	s <- true
}

// Release a permit, returning it to the semaphore.

// Release a permit, increasing the number of available permits by
// one.  If any threads are trying to acquire a permit, then one is
// selected and given the permit that was just released.  That thread
// is (re)enabled for thread scheduling purposes.
// There is no requirement that a thread that releases a permit must
// have acquired that permit by calling Acquire().
// Correct usage of a semaphore is established by programming convention
// in the application.
func (s Semaphore) Release() {
	<-s
}
