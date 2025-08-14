/*
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
package org.apache.pulsar.websocket.data;

public enum ServerError {
    UnknownError,
    MetadataError, // Error with ZK/metadata
    PersistenceError, // Error writing reading from BK
    AuthenticationError, // Non valid authentication
    AuthorizationError, // Not authorized to use resource

    ConsumerBusy, // Unable to subscribe/unsubscribe because
                  // other consumers are connected
    ServiceNotReady, // Any error that requires client retry operation with a fresh lookup
    ProducerBlockedQuotaExceededError, // Unable to create producer because backlog quota exceeded
    ProducerBlockedQuotaExceededException, // Exception while creating producer because quota exceeded
    ChecksumError, // Error while verifying message checksum
    UnsupportedVersionError, // Error when an older client/version doesn't support a required feature
    TopicNotFound, // Topic not found
    SubscriptionNotFound, // Subscription not found
    ConsumerNotFound, // Consumer not found
    TooManyRequests, // Error with too many simultaneously request
    TopicTerminatedError, // The topic has been terminated

    ProducerBusy, // Producer with same name is already connected
    InvalidTopicName, // The topic name is not valid

    IncompatibleSchema, // Specified schema was incompatible with topic schema
    ConsumerAssignError, // Dispatcher assign consumer error

    TransactionCoordinatorNotFound, // Transaction coordinator not found error
    InvalidTxnStatus, // Invalid txn status error
    NotAllowedError, // Not allowed error

    TransactionConflict, // Ack with transaction conflict
    TransactionNotFound, // Transaction not found

    ProducerFenced // When a producer asks and fail to get exclusive producer access,
                   // or loses the eclusive status after a reconnection, the broker will
                   // use this error to indicate that this producer is now permanently
                   // fenced. Applications are now supposed to close it and create a
                   // new producer
}
