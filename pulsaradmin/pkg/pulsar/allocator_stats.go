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

type AllocatorStats struct {
	NumDirectArenas      int              `json:"numDirectArenas"`
	NumHeapArenas        int              `json:"numHeapArenas"`
	NumThreadLocalCaches int              `json:"numThreadLocalCaches"`
	NormalCacheSize      int              `json:"normalCacheSize"`
	SmallCacheSize       int              `json:"smallCacheSize"`
	TinyCacheSize        int              `json:"tinyCacheSize"`
	DirectArenas         []PoolArenaStats `json:"directArenas"`
	HeapArenas           []PoolArenaStats `json:"heapArenas"`
}

type PoolArenaStats struct {
	NumTinySubpages            int                  `json:"numTinySubpages"`
	NumSmallSubpages           int                  `json:"numSmallSubpages"`
	NumChunkLists              int                  `json:"numChunkLists"`
	TinySubpages               []PoolSubpageStats   `json:"tinySubpages"`
	SmallSubpages              []PoolSubpageStats   `json:"smallSubpages"`
	ChunkLists                 []PoolChunkListStats `json:"chunkLists"`
	NumAllocations             int64                `json:"numAllocations"`
	NumTinyAllocations         int64                `json:"numTinyAllocations"`
	NumSmallAllocations        int64                `json:"numSmallAllocations"`
	NumNormalAllocations       int64                `json:"numNormalAllocations"`
	NumHugeAllocations         int64                `json:"numHugeAllocations"`
	NumDeallocations           int64                `json:"numDeallocations"`
	NumTinyDeallocations       int64                `json:"numTinyDeallocations"`
	NumSmallDeallocations      int64                `json:"numSmallDeallocations"`
	NumNormalDeallocations     int64                `json:"numNormalDeallocations"`
	NumHugeDeallocations       int64                `json:"numHugeDeallocations"`
	NumActiveAllocations       int64                `json:"numActiveAllocations"`
	NumActiveTinyAllocations   int64                `json:"numActiveTinyAllocations"`
	NumActiveSmallAllocations  int64                `json:"numActiveSmallAllocations"`
	NumActiveNormalAllocations int64                `json:"numActiveNormalAllocations"`
	NumActiveHugeAllocations   int64                `json:"numActiveHugeAllocations"`
}

type PoolSubpageStats struct {
	MaxNumElements int `json:"maxNumElements"`
	NumAvailable   int `json:"numAvailable"`
	ElementSize    int `json:"elementSize"`
	PageSize       int `json:"pageSize"`
}

type PoolChunkListStats struct {
	MinUsage int              `json:"minUsage"`
	MaxUsage int              `json:"maxUsage"`
	Chunks   []PoolChunkStats `json:"chunks"`
}

type PoolChunkStats struct {
	Usage     int `json:"usage"`
	ChunkSize int `json:"chunkSize"`
	FreeBytes int `json:"freeBytes"`
}
