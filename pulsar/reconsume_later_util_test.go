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

package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelayLevelUtil_GetDelayTime(t *testing.T) {
	a := "1s 5s   10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
	d := NewDelayLevelUtil(a)
	assert.Equal(t, int64(0), d.getDelayTime(0))
	assert.Equal(t, int64(7200000), d.getDelayTime(d.getMaxDelayLevel()))
	assert.Equal(t, int64(7200000), d.getDelayTime(100))
}
