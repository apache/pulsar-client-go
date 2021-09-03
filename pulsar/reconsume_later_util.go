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
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const DefaultMessageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"

type DelayLevelUtil interface {
	GetMaxDelayLevel() int
	GetDelayTime(level int) int64
	ParseDelayLevel() bool
}

type delayLevelUtil struct {
	maxDelayLevel   int
	levelString     string
	delayLevelTable map[int]int64
}

func NewDelayLevelUtil(levelStr string) DelayLevelUtil {
	delayLevelUtil := &delayLevelUtil{
		levelString:     levelStr,
		delayLevelTable: make(map[int]int64),
	}
	delayLevelUtil.ParseDelayLevel()
	return delayLevelUtil
}

func (d *delayLevelUtil) GetMaxDelayLevel() int {
	return d.maxDelayLevel
}

func (d *delayLevelUtil) GetDelayTime(level int) int64 {
	if d.delayLevelTable == nil {
		return 0
	} else if level < 1 {
		return 0
	} else if level > d.maxDelayLevel {
		return d.delayLevelTable[d.maxDelayLevel]
	} else {
		return d.delayLevelTable[level]
	}
}

func (d *delayLevelUtil) ParseDelayLevel() bool {
	d.delayLevelTable = make(map[int]int64)
	timeUnitTable := make(map[string]int64)
	timeUnitTable["s"] = 1000
	timeUnitTable["m"] = 1000 * 60
	timeUnitTable["h"] = 1000 * 60 * 60
	timeUnitTable["d"] = 1000 * 60 * 60 * 24

	levelArray := strings.Split(d.levelString, " ")
	for i := 0; i < len(levelArray); i++ {
		length := len(levelArray[i])
		if length == 0 {
			continue
		}
		timeunit := timeUnitTable[levelArray[i][length-1:]]

		level := i + 1
		if level > d.maxDelayLevel {
			d.maxDelayLevel = level
		}
		num, err := strconv.ParseInt(levelArray[i][:length-1], 10, 64)
		if err != nil {
			log.Error("parseDelayLevel exception" + err.Error())
			log.Info("levelString str = {}", d.levelString)
			return false
		}
		delayTimeMills := timeunit * num
		d.delayLevelTable[level] = delayTimeMills
	}
	return true
}

type reconsumeOptions struct {
	delayTime     int64
	delayTimeUnit time.Duration
	delayLevel    int
}

type ReconsumeOptions interface {
	DelayTime() int64
	DelayTimeUnit() time.Duration
	DelayLevel() int
}

func NewReconsumeOptions() ReconsumeOptions {
	return &reconsumeOptions{
		delayTime:     0,
		delayTimeUnit: time.Millisecond,
		delayLevel:    -2,
	}
}

func NewReconsumeOptionsWithTime(delayTime int64, delayTimeUnit time.Duration) ReconsumeOptions {
	return &reconsumeOptions{
		delayTime:     delayTime,
		delayTimeUnit: delayTimeUnit,
		delayLevel:    -1,
	}
}

func NewReconsumeOptionsWithLevel(delayLevel int) ReconsumeOptions {
	return &reconsumeOptions{
		delayTime:     0,
		delayTimeUnit: time.Millisecond,
		delayLevel:    delayLevel,
	}
}

func (ro *reconsumeOptions) DelayTime() int64 {
	return ro.delayTime
}

func (ro *reconsumeOptions) DelayTimeUnit() time.Duration {
	return ro.delayTimeUnit
}

func (ro *reconsumeOptions) DelayLevel() int {
	return ro.delayLevel
}
