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

type OffloadPolicies struct {
	FileSystemDriver                             bool              `json:"fileSystemDriver"`
	FileSystemProfilePath                        string            `json:"fileSystemProfilePath"`
	FileSystemURI                                string            `json:"fileSystemURI"`
	GcsDriver                                    bool              `json:"gcsDriver"`
	GcsManagedLedgerOffloadBucket                string            `json:"gcsManagedLedgerOffloadBucket"`
	GcsManagedLedgerOffloadMaxBlockSizeInBytes   int               `json:"gcsManagedLedgerOffloadMaxBlockSizeInBytes"`
	GcsManagedLedgerOffloadReadBufferSizeInBytes int               `json:"gcsManagedLedgerOffloadReadBufferSizeInBytes"`
	GcsManagedLedgerOffloadRegion                string            `json:"gcsManagedLedgerOffloadRegion"`
	GcsManagedLedgerOffloadServiceAccountKeyFile string            `json:"gcsManagedLedgerOffloadServiceAccountKeyFile"`
	ManagedLedgerExtraConfigurations             map[string]string `json:"managedLedgerExtraConfigurations"`
	ManagedLedgerOffloadBucket                   string            `json:"managedLedgerOffloadBucket"`
	ManagedLedgerOffloadDeletionLagInMillis      int               `json:"managedLedgerOffloadDeletionLagInMillis"`
	ManagedLedgerOffloadDriver                   string            `json:"managedLedgerOffloadDriver"`
	ManagedLedgerOffloadMaxBlockSizeInBytes      int               `json:"managedLedgerOffloadMaxBlockSizeInBytes"`
	ManagedLedgerOffloadMaxThreads               int               `json:"managedLedgerOffloadMaxThreads"`
	ManagedLedgerOffloadPrefetchRounds           int               `json:"managedLedgerOffloadPrefetchRounds"`
	ManagedLedgerOffloadReadBufferSizeInBytes    int               `json:"managedLedgerOffloadReadBufferSizeInBytes"`
	ManagedLedgerOffloadRegion                   string            `json:"managedLedgerOffloadRegion"`
	ManagedLedgerOffloadServiceEndpoint          string            `json:"managedLedgerOffloadServiceEndpoint"`
	ManagedLedgerOffloadThresholdInBytes         int               `json:"managedLedgerOffloadThresholdInBytes"`
	ManagedLedgerOffloadThresholdInSeconds       int               `json:"managedLedgerOffloadThresholdInSeconds"`
	ManagedLedgerOffloadedReadPriority           string            `json:"managedLedgerOffloadedReadPriority"`
	OffloadersDirectory                          string            `json:"offloadersDirectory"`
	S3Driver                                     bool              `json:"s3Driver"`
	S3ManagedLedgerOffloadBucket                 string            `json:"s3ManagedLedgerOffloadBucket"`
	S3ManagedLedgerOffloadCredentialID           string            `json:"s3ManagedLedgerOffloadCredentialId"`
	S3ManagedLedgerOffloadCredentialSecret       string            `json:"s3ManagedLedgerOffloadCredentialSecret"`
	S3ManagedLedgerOffloadMaxBlockSizeInBytes    int               `json:"s3ManagedLedgerOffloadMaxBlockSizeInBytes"`
	S3ManagedLedgerOffloadReadBufferSizeInBytes  int               `json:"s3ManagedLedgerOffloadReadBufferSizeInBytes"`
	S3ManagedLedgerOffloadRegion                 string            `json:"s3ManagedLedgerOffloadRegion"`
	S3ManagedLedgerOffloadRole                   string            `json:"s3ManagedLedgerOffloadRole"`
	S3ManagedLedgerOffloadRoleSessionName        string            `json:"s3ManagedLedgerOffloadRoleSessionName"`
	S3ManagedLedgerOffloadServiceEndpoint        string            `json:"s3ManagedLedgerOffloadServiceEndpoint"`
}
