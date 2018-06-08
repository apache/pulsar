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

package pulsar

type Result int

const (
	UnknownError                          Result = 1  // Unknown error happened on broker
	InvalidConfiguration                  Result = 2  // Invalid configuration
	TimeoutError                          Result = 3  // Operation timed out
	LookupError                           Result = 4  // Broker lookup failed
	ConnectError                          Result = 5  // Failed to connect to broker
	ReadError                             Result = 6  // Failed to read from socket
	AuthenticationError                   Result = 7  // Authentication failed on broker
	AuthorizationError                    Result = 8  // Client is not authorized to create producer/consumer
	ErrorGettingAuthenticationData        Result = 9  // Client cannot find authorization data
	BrokerMetadataError                   Result = 10 // Broker failed in updating metadata
	BrokerPersistenceError                Result = 11 // Broker failed to persist entry
	ChecksumError                         Result = 12 // Corrupt message checksum failure
	ConsumerBusy                          Result = 13 // Exclusive consumer is already connected
	NotConnectedError                     Result = 14 // Producer/Consumer is not currently connected to broker
	AlreadyClosedError                    Result = 15 // Producer/Consumer is already closed and not accepting any operation
	InvalidMessage                        Result = 16 // Error in publishing an already used message
	ConsumerNotInitialized                Result = 17 // Consumer is not initialized
	ProducerNotInitialized                Result = 18 // Producer is not initialized
	TooManyLookupRequestException         Result = 19 // Too Many concurrent LookupRequest
	InvalidTopicName                      Result = 20 // Invalid topic name
	InvalidUrl                            Result = 21 // Client Initialized with Invalid Broker Url (VIP Url passed to Client Constructor)
	ServiceUnitNotReady                   Result = 22 // Service Unit unloaded between client did lookup and producer/consumer got created
	OperationNotSupported                 Result = 23
	ProducerBlockedQuotaExceededError     Result = 24 // Producer is blocked
	ProducerBlockedQuotaExceededException Result = 25 // Producer is getting exception
	ProducerQueueIsFull                   Result = 26 // Producer queue is full
	MessageTooBig                         Result = 27 // Trying to send a messages exceeding the max size
	TopicNotFound                         Result = 28 // Topic not found
	SubscriptionNotFound                  Result = 29 // Subscription not found
	ConsumerNotFound                      Result = 30 // Consumer not found
	UnsupportedVersionError               Result = 31 // Error when an older client/version doesn't support a required feature
	TopicTerminated                       Result = 32 // Topic was already terminated
	CryptoError                           Result = 33 // Error when crypto operation fails
)
