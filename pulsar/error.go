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

import "fmt"

// Result used to represent pulsar processing is an alias of type int.
type Result int

const (
	// ResultOk means no errors
	ResultOk = iota
	// ResultUnknownError means unknown error happened on broker
	ResultUnknownError
	// ResultInvalidConfiguration means invalid configuration
	ResultInvalidConfiguration
	// ResultTimeoutError means operation timed out
	ResultTimeoutError
	// ResultLookupError means broker lookup failed
	ResultLookupError
	// ResultInvalidTopicName means invalid topic name
	ResultInvalidTopicName
	// ResultConnectError means failed to connect to broker
	ResultConnectError

	//ReadError                      Result = 6  // Failed to read from socket
	//AuthenticationError            Result = 7  // Authentication failed on broker
	//AuthorizationError             Result = 8  // Client is not authorized to create producer/consumer
	//ErrorGettingAuthenticationData Result = 9  // Client cannot find authorization data
	//BrokerMetadataError            Result = 10 // Broker failed in updating metadata
	//BrokerPersistenceError         Result = 11 // Broker failed to persist entry
	//ChecksumError                  Result = 12 // Corrupt message checksum failure
	//ConsumerBusy                   Result = 13 // Exclusive consumer is already connected
	//NotConnectedError              Result = 14 // Producer/Consumer is not currently connected to broker
	//AlreadyClosedError             Result = 15 // Producer/Consumer is already closed and not accepting any operation
	//InvalidMessage                 Result = 16 // Error in publishing an already used message
	//ConsumerNotInitialized         Result = 17 // Consumer is not initialized
	//ProducerNotInitialized         Result = 18 // Producer is not initialized
	//TooManyLookupRequestException  Result = 19 // Too Many concurrent LookupRequest
	//InvalidUrl                            Result = 21 // Client Initialized with Invalid Broker Url (VIP Url passed to Client Constructor)
	//ServiceUnitNotReady                   Result = 22 // Service Unit unloaded between client did lookup and producer/consumer got created
	//OperationNotSupported                 Result = 23
	//ProducerBlockedQuotaExceededError     Result = 24 // Producer is blocked
	//ProducerBlockedQuotaExceededException Result = 25 // Producer is getting exception
	//ProducerQueueIsFull                   Result = 26 // Producer queue is full
	//MessageTooBig                         Result = 27 // Trying to send a messages exceeding the max size
	//TopicNotFound                         Result = 28 // Topic not found
	//SubscriptionNotFound                  Result = 29 // Subscription not found
	//ConsumerNotFound                      Result = 30 // Consumer not found
	//UnsupportedVersionError               Result = 31 // Error when an older client/version doesn't support a required feature
	//TopicTerminated                       Result = 32 // Topic was already terminated
	//CryptoError                           Result = 33 // Error when crypto operation fails
)

// Error implement error interface, composed of two parts: msg and result.
type Error struct {
	msg    string
	result Result
}

func (e *Error) Result() Result {
	return e.result
}

func (e *Error) Error() string {
	return e.msg
}

func newError(result Result, msg string) error {
	return &Error{
		msg:    fmt.Sprintf("%s: %s", msg, getResultStr(result)),
		result: result,
	}
}

func getResultStr(r Result) string {
	switch r {
	case ResultOk:
		return "OK"
	case ResultUnknownError:
		return "Unknown error"
	case ResultInvalidConfiguration:
		return "InvalidConfiguration"
	case ResultTimeoutError:
		return "TimeoutError"
	case ResultLookupError:
		return "LookupError"
	case ResultInvalidTopicName:
		return "InvalidTopicName"
	case ResultConnectError:
		return "ConnectError"
	default:
		return fmt.Sprintf("Result(%d)", r)
	}
}
