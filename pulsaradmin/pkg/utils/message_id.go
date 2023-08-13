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

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type MessageID struct {
	LedgerID       int64 `json:"ledgerId"`
	EntryID        int64 `json:"entryId"`
	PartitionIndex int   `json:"partitionIndex"`
	BatchIndex     int   `json:"-"`
}

var Latest = MessageID{0x7fffffffffffffff, 0x7fffffffffffffff, -1, -1}
var Earliest = MessageID{-1, -1, -1, -1}

func ParseMessageID(str string) (*MessageID, error) {
	s := strings.Split(str, ":")

	m := Earliest

	if len(s) < 2 || len(s) > 4 {
		return nil, errors.Errorf("invalid message id string. %s", str)
	}

	ledgerID, err := strconv.ParseInt(s[0], 10, 64)
	if err != nil {
		return nil, errors.Errorf("invalid ledger id. %s", str)
	}
	m.LedgerID = ledgerID

	entryID, err := strconv.ParseInt(s[1], 10, 64)
	if err != nil {
		return nil, errors.Errorf("invalid entry id. %s", str)
	}
	m.EntryID = entryID

	if len(s) > 2 {
		pi, err := strconv.Atoi(s[2])
		if err != nil {
			return nil, errors.Errorf("invalid partition index. %s", str)
		}
		m.PartitionIndex = pi
	}

	if len(s) == 4 {
		bi, err := strconv.Atoi(s[3])
		if err != nil {
			return nil, errors.Errorf("invalid batch index. %s", str)
		}
		m.BatchIndex = bi
	}

	return &m, nil
}

func (m MessageID) String() string {
	return strconv.FormatInt(m.LedgerID, 10) + ":" +
		strconv.FormatInt(m.EntryID, 10) + ":" +
		strconv.Itoa(m.PartitionIndex) + ":" +
		strconv.Itoa(m.BatchIndex)
}
