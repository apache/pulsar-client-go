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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseRelativeTimeInSeconds(t *testing.T) {
	testSecondStr := "10s"
	timestamp, err := ParseRelativeTimeInSeconds(testSecondStr)
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(10)*time.Second, timestamp)

	testMinuteStr := "10m"
	timestamp, err = ParseRelativeTimeInSeconds(testMinuteStr)
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(10)*time.Minute, timestamp)

	testHourStr := "10h"
	timestamp, err = ParseRelativeTimeInSeconds(testHourStr)
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(10)*time.Hour, timestamp)

	testDaysStr := "10d"
	timestamp, err = ParseRelativeTimeInSeconds(testDaysStr)
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(10)*time.Hour*24, timestamp)

	testWeekStr := "10w"
	timestamp, err = ParseRelativeTimeInSeconds(testWeekStr)
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(10)*time.Hour*24*7, timestamp)

	testYearStr := "10y"
	timestamp, err = ParseRelativeTimeInSeconds(testYearStr)
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(10)*time.Hour*24*7*365, timestamp)
}

func TestTimestampMillis(t *testing.T) {
	assert.Equal(t, uint64(0), TimestampMillis(time.Time{}))
	assert.Equal(t, uint64(7), TimestampMillis(time.Unix(0, 7*int64(time.Millisecond))))
}
