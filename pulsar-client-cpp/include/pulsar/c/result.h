/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <pulsar/defines.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    pulsar_result_Ok,  /// Operation successful

    pulsar_result_UnknownError,  /// Unknown error happened on broker

    pulsar_result_InvalidConfiguration,  /// Invalid configuration

    pulsar_result_Timeout,       /// Operation timed out
    pulsar_result_LookupError,   /// Broker lookup failed
    pulsar_result_ConnectError,  /// Failed to connect to broker
    pulsar_result_ReadError,     /// Failed to read from socket

    pulsar_result_AuthenticationError,             /// Authentication failed on broker
    pulsar_result_AuthorizationError,              /// Client is not authorized to create producer/consumer
    pulsar_result_ErrorGettingAuthenticationData,  /// Client cannot find authorization data

    pulsar_result_BrokerMetadataError,     /// Broker failed in updating metadata
    pulsar_result_BrokerPersistenceError,  /// Broker failed to persist entry
    pulsar_result_ChecksumError,           /// Corrupt message checksum failure

    pulsar_result_ConsumerBusy,   /// Exclusive consumer is already connected
    pulsar_result_NotConnected,   /// Producer/Consumer is not currently connected to broker
    pulsar_result_AlreadyClosed,  /// Producer/Consumer is already closed and not accepting any operation

    pulsar_result_InvalidMessage,  /// Error in publishing an already used message

    pulsar_result_ConsumerNotInitialized,         /// Consumer is not initialized
    pulsar_result_ProducerNotInitialized,         /// Producer is not initialized
    pulsar_result_TooManyLookupRequestException,  /// Too Many concurrent LookupRequest

    pulsar_result_InvalidTopicName,  /// Invalid topic name
    pulsar_result_InvalidUrl,        /// Client Initialized with Invalid Broker Url (VIP Url passed to Client
                                     /// Constructor)
    pulsar_result_ServiceUnitNotReady,  /// Service Unit unloaded between client did lookup and
                                        /// producer/consumer got created

    pulsar_result_OperationNotSupported,
    pulsar_result_ProducerBlockedQuotaExceededError,      /// Producer is blocked
    pulsar_result_ProducerBlockedQuotaExceededException,  /// Producer is getting exception
    pulsar_result_ProducerQueueIsFull,                    /// Producer queue is full
    pulsar_result_MessageTooBig,                          /// Trying to send a messages exceeding the max size
    pulsar_result_TopicNotFound,                          /// Topic not found
    pulsar_result_SubscriptionNotFound,                   /// Subscription not found
    pulsar_result_ConsumerNotFound,                       /// Consumer not found
    pulsar_result_UnsupportedVersionError,  /// Error when an older client/version doesn't support a required
                                            /// feature
    pulsar_result_TopicTerminated,          /// Topic was already terminated
    pulsar_result_CryptoError               /// Error when crypto operation fails
} pulsar_result;

// Return string representation of result code
PULSAR_PUBLIC const char *pulsar_result_str(pulsar_result result);

#ifdef __cplusplus
}
#endif
