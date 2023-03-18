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

type PersistencePolicies struct {
	BookkeeperEnsemble             int     `json:"bookkeeperEnsemble"`
	BookkeeperWriteQuorum          int     `json:"bookkeeperWriteQuorum"`
	BookkeeperAckQuorum            int     `json:"bookkeeperAckQuorum"`
	ManagedLedgerMaxMarkDeleteRate float64 `json:"managedLedgerMaxMarkDeleteRate"`
}

func NewPersistencePolicies(bookkeeperEnsemble, bookkeeperWriteQuorum, bookkeeperAckQuorum int,
	managedLedgerMaxMarkDeleteRate float64) PersistencePolicies {
	return PersistencePolicies{
		BookkeeperEnsemble:             bookkeeperEnsemble,
		BookkeeperWriteQuorum:          bookkeeperWriteQuorum,
		BookkeeperAckQuorum:            bookkeeperAckQuorum,
		ManagedLedgerMaxMarkDeleteRate: managedLedgerMaxMarkDeleteRate,
	}
}

type BookieAffinityGroupData struct {
	BookkeeperAffinityGroupPrimary   string `json:"bookkeeperAffinityGroupPrimary"`
	BookkeeperAffinityGroupSecondary string `json:"bookkeeperAffinityGroupSecondary"`
}
