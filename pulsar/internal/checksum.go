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

import "hash/crc32"

// crc32cTable holds the precomputed crc32 hash table
// used by Pulsar (crc32c)
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Crc32cCheckSum handles computing the checksum.
func Crc32cCheckSum(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}
