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

//nolint:lll
type OffloadPolicies struct {
	OffloadersDirectory                               string            `json:"offloadersDirectory,omitempty"`
	ManagedLedgerOffloadDriver                        string            `json:"managedLedgerOffloadDriver,omitempty"`
	ManagedLedgerOffloadMaxThreads                    int               `json:"managedLedgerOffloadMaxThreads,omitempty"`
	ManagedLedgerOffloadReadThreads                   int               `json:"managedLedgerOffloadReadThreads,omitempty"`
	ManagedLedgerOffloadPrefetchRounds                int               `json:"managedLedgerOffloadPrefetchRounds,omitempty"`
	ManagedLedgerOffloadThresholdInSeconds            int64             `json:"managedLedgerOffloadThresholdInSeconds,omitempty"`
	ManagedLedgerOffloadThresholdInBytes              int64             `json:"managedLedgerOffloadThresholdInBytes,omitempty"`
	ManagedLedgerOffloadDeletionLagInMillis           int64             `json:"managedLedgerOffloadDeletionLagInMillis,omitempty"`
	ManagedLedgerOffloadedReadPriority                string            `json:"managedLedgerOffloadedReadPriority,omitempty"`
	ManagedLedgerExtraConfigurations                  map[string]string `json:"managedLedgerExtraConfigurations,omitempty"`
	ManagedLedgerOffloadAutoTriggerSizeThresholdBytes int64             `json:"managedLedgerOffloadAutoTriggerSizeThresholdBytes,omitempty"`
	S3ManagedLedgerOffloadRegion                      string            `json:"s3ManagedLedgerOffloadRegion,omitempty"`
	S3ManagedLedgerOffloadBucket                      string            `json:"s3ManagedLedgerOffloadBucket,omitempty"`
	S3ManagedLedgerOffloadServiceEndpoint             string            `json:"s3ManagedLedgerOffloadServiceEndpoint,omitempty"`
	S3ManagedLedgerOffloadMaxBlockSizeInBytes         int               `json:"s3ManagedLedgerOffloadMaxBlockSizeInBytes,omitempty"`
	S3ManagedLedgerOffloadReadBufferSizeInBytes       int               `json:"s3ManagedLedgerOffloadReadBufferSizeInBytes,omitempty"`
	S3ManagedLedgerOffloadCredentialID                string            `json:"s3ManagedLedgerOffloadCredentialId,omitempty"`
	S3ManagedLedgerOffloadCredentialSecret            string            `json:"s3ManagedLedgerOffloadCredentialSecret,omitempty"`
	S3ManagedLedgerOffloadRole                        string            `json:"s3ManagedLedgerOffloadRole,omitempty"`
	S3ManagedLedgerOffloadRoleSessionName             string            `json:"s3ManagedLedgerOffloadRoleSessionName,omitempty"`
	GCSManagedLedgerOffloadRegion                     string            `json:"gcsManagedLedgerOffloadRegion,omitempty"`
	GCSManagedLedgerOffloadBucket                     string            `json:"gcsManagedLedgerOffloadBucket,omitempty"`
	GCSManagedLedgerOffloadMaxBlockSizeInBytes        int               `json:"gcsManagedLedgerOffloadMaxBlockSizeInBytes,omitempty"`
	GCSManagedLedgerOffloadReadBufferSizeInBytes      int               `json:"gcsManagedLedgerOffloadReadBufferSizeInBytes,omitempty"`
	GCSManagedLedgerOffloadServiceAccountKeyFile      string            `json:"gcsManagedLedgerOffloadServiceAccountKeyFile,omitempty"`
	FileSystemProfilePath                             string            `json:"fileSystemProfilePath,omitempty"`
	FileSystemURI                                     string            `json:"fileSystemURI,omitempty"`
	ManagedLedgerOffloadBucket                        string            `json:"managedLedgerOffloadBucket,omitempty"`
	ManagedLedgerOffloadRegion                        string            `json:"managedLedgerOffloadRegion,omitempty"`
	ManagedLedgerOffloadServiceEndpoint               string            `json:"managedLedgerOffloadServiceEndpoint,omitempty"`
	ManagedLedgerOffloadMaxBlockSizeInBytes           int               `json:"managedLedgerOffloadMaxBlockSizeInBytes,omitempty"`
	ManagedLedgerOffloadReadBufferSizeInBytes         int               `json:"managedLedgerOffloadReadBufferSizeInBytes,omitempty"`
	ManagedLedgerOffloadDriverMetadata                map[string]string `json:"managedLedgerOffloadDriverMetadata,omitempty"`
}

func NewOffloadPolicies() *OffloadPolicies {
	return &OffloadPolicies{
		ManagedLedgerOffloadMaxThreads:                    2,
		ManagedLedgerOffloadThresholdInBytes:              -1,
		ManagedLedgerOffloadDeletionLagInMillis:           14400000, // 4 hours
		ManagedLedgerOffloadAutoTriggerSizeThresholdBytes: -1,
		ManagedLedgerExtraConfigurations:                  make(map[string]string),
		ManagedLedgerOffloadDriverMetadata:                make(map[string]string),
	}
}
