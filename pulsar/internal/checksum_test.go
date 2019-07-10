//
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
//

package internal

import (
    "bytes"
    "hash/crc32"
    "testing"
)

func TestFrameChecksum(t *testing.T) {
    input := []byte{1, 2, 3, 4, 5}
    var f CheckSum

    if got := f.compute(); got != nil {
        t.Fatalf("compute() = %v; expected nil", got)
    }

    if _, err := f.Write(input); err != nil {
        t.Fatalf("Write() err = %v; expected nil", err)
    }

    h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
    if _, err := h.Write(input); err != nil {
        t.Fatal(err)
    }

    if got, expected := f.compute(), h.Sum(nil); !bytes.Equal(got, expected) {
        t.Fatalf("compute() = %x; expected %x", got, expected)
    } else {
        t.Logf("compute() = 0x%x", got)
    }
}

