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
#ifndef ERROR_HPP_
#define ERROR_HPP_

#include <iosfwd>
#include <pulsar/defines.h>

namespace pulsar {

/**
 * Collection of return codes
 */
enum Result
{
    ResultOk,  /// Operation successful

    ResultUnknownError,  /// Unknown error happened on broker

    ResultInvalidConfiguration,  /// Invalid configuration

    ResultTimeout,       /// Operation timed out
    ResultLookupError,   /// Broker lookup failed
    ResultConnectError,  /// Failed to connect to broker
    ResultReadError,     /// Failed to read from socket

    ResultAuthenticationError,             /// Authentication failed on broker
    ResultAuthorizationError,              /// Client is not authorized to create producer/consumer
    ResultErrorGettingAuthenticationData,  /// Client cannot find authorization data

    ResultBrokerMetadataError,     /// Broker failed in updating metadata
    ResultBrokerPersistenceError,  /// Broker failed to persist entry
    ResultChecksumError,           /// Corrupt message checksum failure

    ResultConsumerBusy,   /// Exclusive consumer is already connected
    ResultNotConnected,   /// Producer/Consumer is not currently connected to broker
    ResultAlreadyClosed,  /// Producer/Consumer is already closed and not accepting any operation

    ResultInvalidMessage,  /// Error in publishing an already used message

    ResultConsumerNotInitialized,         /// Consumer is not initialized
    ResultProducerNotInitialized,         /// Producer is not initialized
    ResultProducerBusy,                   /// Producer with same name is already connected
    ResultTooManyLookupRequestException,  /// Too Many concurrent LookupRequest

    ResultInvalidTopicName,  /// Invalid topic name
    ResultInvalidUrl,  /// Client Initialized with Invalid Broker Url (VIP Url passed to Client Constructor)
    ResultServiceUnitNotReady,  /// Service Unit unloaded between client did lookup and producer/consumer got
                                /// created
    ResultOperationNotSupported,
    ResultProducerBlockedQuotaExceededError,      /// Producer is blocked
    ResultProducerBlockedQuotaExceededException,  /// Producer is getting exception
    ResultProducerQueueIsFull,                    /// Producer queue is full
    ResultMessageTooBig,                          /// Trying to send a messages exceeding the max size
    ResultTopicNotFound,                          /// Topic not found
    ResultSubscriptionNotFound,                   /// Subscription not found
    ResultConsumerNotFound,                       /// Consumer not found
    ResultUnsupportedVersionError,  /// Error when an older client/version doesn't support a required feature
    ResultTopicTerminated,          /// Topic was already terminated
    ResultCryptoError,              /// Error when crypto operation fails

    ResultIncompatibleSchema,   /// Specified schema is incompatible with the topic's schema
    ResultConsumerAssignError,  /// Error when a new consumer connected but can't assign messages to this
                                /// consumer
};

// Return string representation of result code
PULSAR_PUBLIC const char* strResult(Result result);
}  // namespace pulsar

PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, pulsar::Result result);

#endif /* ERROR_HPP_ */
