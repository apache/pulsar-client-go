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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type People interface {
	MakeSound() string
}

type Student struct{}

func (s *Student) MakeSound() string {
	return "Student"
}

type Teacher struct{}

func (t Teacher) MakeSound() string {
	return "Teacher"
}

//nolint
func TestIsNilFixed(t *testing.T) {
	var stu *Student = nil
	var people People
	people = stu

	var teacher Teacher
	people = teacher

	assert.False(t, IsNilFixed(people))

	var m map[string]string
	assert.True(t, IsNilFixed(m))

	var s []string
	assert.True(t, IsNilFixed(s))

	var ch chan string
	assert.True(t, IsNilFixed(ch))

	var nilInterface People
	assert.True(t, IsNilFixed(nilInterface))

	// pointer to an interface, the IsNilFixed method cannot check this.
	assert.False(t, IsNilFixed(&nilInterface))
}
