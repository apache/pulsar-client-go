// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

type FunctionStats struct {
	// Overall total number of records function received from source
	ReceivedTotal int64 `json:"receivedTotal"`

	// Overall total number of records successfully processed by user function
	ProcessedSuccessfullyTotal int64 `json:"processedSuccessfullyTotal"`

	// Overall total number of system exceptions thrown
	SystemExceptionsTotal int64 `json:"systemExceptionsTotal"`

	// Overall total number of user exceptions thrown
	UserExceptionsTotal int64 `json:"userExceptionsTotal"`

	// Average process latency for function
	AvgProcessLatency float64 `json:"avgProcessLatency"`

	// Timestamp of when the function was last invoked by any instance
	LastInvocation int64 `json:"lastInvocation"`

	OneMin FunctionInstanceStatsDataBase `json:"oneMin"`

	Instances []FunctionInstanceStats `json:"instances"`

	FunctionInstanceStats
}

type FunctionInstanceStats struct {
	FunctionInstanceStatsDataBase

	InstanceID int64 `json:"instanceId"`

	Metrics FunctionInstanceStatsData `json:"metrics"`
}

type FunctionInstanceStatsDataBase struct {
	// Total number of records function received from source for instance
	ReceivedTotal int64 `json:"receivedTotal"`

	// Total number of records successfully processed by user function for instance
	ProcessedSuccessfullyTotal int64 `json:"processedSuccessfullyTotal"`

	// Total number of system exceptions thrown for instance
	SystemExceptionsTotal int64 `json:"systemExceptionsTotal"`

	// Total number of user exceptions thrown for instance
	UserExceptionsTotal int64 `json:"userExceptionsTotal"`

	// Average process latency for function for instance
	AvgProcessLatency float64 `json:"avgProcessLatency"`
}

type FunctionInstanceStatsData struct {
	OneMin FunctionInstanceStatsDataBase `json:"oneMin"`

	// Timestamp of when the function was last invoked for instance
	LastInvocation int64 `json:"lastInvocation"`

	// Map of user defined metrics
	UserMetrics map[string]float64 `json:"userMetrics"`

	FunctionInstanceStatsDataBase
}

func (fs *FunctionStats) AddInstance(functionInstanceStats FunctionInstanceStats) {
	fs.Instances = append(fs.Instances, functionInstanceStats)
}

func (fs *FunctionStats) CalculateOverall() *FunctionStats {
	var (
		nonNullInstances       int
		nonNullInstancesOneMin int
	)

	for _, functionInstanceStats := range fs.Instances {
		functionInstanceStatsData := functionInstanceStats.Metrics
		fs.ReceivedTotal += functionInstanceStatsData.ReceivedTotal
		fs.ProcessedSuccessfullyTotal += functionInstanceStatsData.ProcessedSuccessfullyTotal
		fs.SystemExceptionsTotal += functionInstanceStatsData.SystemExceptionsTotal
		fs.UserExceptionsTotal += functionInstanceStatsData.UserExceptionsTotal

		if functionInstanceStatsData.AvgProcessLatency != 0 {
			if fs.AvgProcessLatency == 0 {
				fs.AvgProcessLatency = 0.0
			}

			fs.AvgProcessLatency += functionInstanceStatsData.AvgProcessLatency
			nonNullInstances++
		}

		fs.OneMin.ReceivedTotal += functionInstanceStatsData.OneMin.ReceivedTotal
		fs.OneMin.ProcessedSuccessfullyTotal += functionInstanceStatsData.OneMin.ProcessedSuccessfullyTotal
		fs.OneMin.SystemExceptionsTotal += functionInstanceStatsData.OneMin.SystemExceptionsTotal
		fs.OneMin.UserExceptionsTotal += functionInstanceStatsData.OneMin.UserExceptionsTotal

		if functionInstanceStatsData.OneMin.AvgProcessLatency != 0 {
			if fs.OneMin.AvgProcessLatency == 0 {
				fs.OneMin.AvgProcessLatency = 0.0
			}

			fs.OneMin.AvgProcessLatency += functionInstanceStatsData.OneMin.AvgProcessLatency
			nonNullInstancesOneMin++
		}

		if functionInstanceStatsData.LastInvocation != 0 {
			if fs.LastInvocation == 0 || functionInstanceStatsData.LastInvocation > fs.LastInvocation {
				fs.LastInvocation = functionInstanceStatsData.LastInvocation
			}
		}
	}

	// calculate average from sum
	if nonNullInstances > 0 {
		fs.AvgProcessLatency /= float64(nonNullInstances)
	} else {
		fs.AvgProcessLatency = 0
	}

	// calculate 1min average from sum
	if nonNullInstancesOneMin > 0 {
		fs.OneMin.AvgProcessLatency /= float64(nonNullInstancesOneMin)
	} else {
		fs.AvgProcessLatency = 0
	}

	return fs
}
