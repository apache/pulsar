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
package org.apache.pulsar.client.api;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Base type of exception thrown by Pulsar client.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@SuppressWarnings("serial")
public class PulsarClientException extends IOException {
    private long sequenceId = -1;
    private AtomicInteger previousExceptionAttempt;

    /**
     * Constructs an {@code PulsarClientException} with the specified detail message.
     *
     * @param msg
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     */
    public PulsarClientException(String msg) {
        super(msg);
    }

    /**
     * Constructs an {@code PulsarClientException} with the specified detail message.
     *
     * @param msg
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     * @param sequenceId
     *        The sequenceId of the message
     */
    public PulsarClientException(String msg, long sequenceId) {
        super(msg);
        this.sequenceId = sequenceId;
    }

    /**
     * Constructs an {@code PulsarClientException} with the specified cause.
     *
     * @param t
     *        The cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A null value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     */
    public PulsarClientException(Throwable t) {
        super(t);
    }

    /**
     * Constructs an {@code PulsarClientException} with the specified cause.
     *
     * @param msg
     *            The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     *
     * @param t
     *            The cause (which is saved for later retrieval by the {@link #getCause()} method). (A null value is
     *            permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public PulsarClientException(String msg, Throwable t) {
        super(msg, t);
    }

    public void setPreviousExceptionCount(AtomicInteger previousExceptionCount) {
        this.previousExceptionAttempt = previousExceptionCount;
    }

    @Override
    public String toString() {
        if (previousExceptionAttempt == null || previousExceptionAttempt.get() == 0) {
            return super.toString();
        } else {
            return super.toString() + ", previous-attempt: " + previousExceptionAttempt;
        }
    }
    /**
     * Constructs an {@code PulsarClientException} with the specified cause.
     *
     * @param t
     *            The cause (which is saved for later retrieval by the {@link #getCause()} method). (A null value is
     *            permitted, and indicates that the cause is nonexistent or unknown.)
     * @param sequenceId
     *            The sequenceId of the message
     */
    public PulsarClientException(Throwable t, long sequenceId) {
        super(t);
        this.sequenceId = sequenceId;
    }

    /**
     * Invalid Service URL exception thrown by Pulsar client.
     */
    public static class InvalidServiceURL extends PulsarClientException {
        /**
         * Constructs an {@code InvalidServiceURL} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public InvalidServiceURL(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code InvalidServiceURL} with the specified cause.
         *
         *@param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public InvalidServiceURL(String msg, Throwable t) {
            super(msg, t);
        }
    }

    /**
     * Invalid Configuration exception thrown by Pulsar client.
     */
    public static class InvalidConfigurationException extends PulsarClientException {
        /**
         * Constructs an {@code InvalidConfigurationException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public InvalidConfigurationException(String msg) {
            super(msg);
        }

        /**
         * Constructs an {@code InvalidConfigurationException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public InvalidConfigurationException(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code InvalidConfigurationException} with the specified cause.
         *
         *@param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public InvalidConfigurationException(String msg, Throwable t) {
            super(msg, t);
        }
    }

    /**
     * Not Found exception thrown by Pulsar client.
     */
    public static class NotFoundException extends PulsarClientException {
        /**
         * Constructs an {@code NotFoundException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public NotFoundException(String msg) {
            super(msg);
        }

        /**
         * Constructs an {@code NotFoundException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public NotFoundException(Throwable t) {
            super(t);
        }
    }

    /**
     * Timeout exception thrown by Pulsar client.
     */
    public static class TimeoutException extends PulsarClientException {
        /**
         * Constructs an {@code TimeoutException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public TimeoutException(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code TimeoutException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         * @param sequenceId
         *        The sequenceId of the message
         */
        public TimeoutException(Throwable t, long sequenceId) {
            super(t, sequenceId);
        }

        /**
         * Constructs an {@code TimeoutException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public TimeoutException(String msg) {
            super(msg);
        }

        /**
         * Constructs an {@code TimeoutException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public TimeoutException(String msg, long sequenceId) {
            super(msg, sequenceId);
        }

    }

    /**
     * Incompatible schema exception thrown by Pulsar client.
     */
    public static class IncompatibleSchemaException extends PulsarClientException {
        /**
         * Constructs an {@code IncompatibleSchemaException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public IncompatibleSchemaException(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code IncompatibleSchemaException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public IncompatibleSchemaException(String msg) {
            super(msg);
        }
    }

    /**
     * Topic does not exist and cannot be created.
     */
    public static class TopicDoesNotExistException extends PulsarClientException {
        /**
         * Constructs an {@code TopicDoesNotExistException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public TopicDoesNotExistException(String msg) {
            super(msg);
        }
    }

    /**
     *  Not found subscription that cannot be created.
     */
    public static class SubscriptionNotFoundException extends PulsarClientException {
        /**
         * Constructs an {@code SubscriptionNotFoundException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public SubscriptionNotFoundException(String msg) {
            super(msg);
        }
    }

    /**
     * Lookup exception thrown by Pulsar client.
     */
    public static class LookupException extends PulsarClientException {
        /**
         * Constructs an {@code LookupException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public LookupException(String msg) {
            super(msg);
        }
    }

    /**
     * Too many requests exception thrown by Pulsar client.
     */
    public static class TooManyRequestsException extends LookupException {
        /**
         * Constructs an {@code TooManyRequestsException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public TooManyRequestsException(String msg) {
            super(msg);
        }
    }

    /**
     * Connect exception thrown by Pulsar client.
     */
    public static class ConnectException extends PulsarClientException {
        /**
         * Constructs an {@code ConnectException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public ConnectException(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code ConnectException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public ConnectException(String msg) {
            super(msg);
        }
    }

    /**
     * Already closed exception thrown by Pulsar client.
     */
    public static class AlreadyClosedException extends PulsarClientException {
        /**
         * Constructs an {@code AlreadyClosedException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public AlreadyClosedException(String msg) {
            super(msg);
        }

        /**
         * Constructs an {@code AlreadyClosedException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         * @param sequenceId
         *        The sequenceId of the message
         */
        public AlreadyClosedException(String msg, long sequenceId) {
            super(msg, sequenceId);
        }
    }

    /**
     * Topic terminated exception thrown by Pulsar client.
     */
    public static class TopicTerminatedException extends PulsarClientException {
        /**
         * Constructs an {@code TopicTerminatedException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public TopicTerminatedException(String msg) {
            super(msg);
        }

        /**
         * Constructs an {@code TopicTerminatedException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         * @param sequenceId
         *        The sequenceId of the message
         */
        public TopicTerminatedException(String msg, long sequenceId) {
            super(msg, sequenceId);
        }
    }

    /**
     * TopicMigration exception thrown by Pulsar client.
     */
    public static class TopicMigrationException extends PulsarClientException {
        /**
         * Constructs an {@code TopicMigrationException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public TopicMigrationException(String msg) {
            super(msg);
        }
    }

    /**
     * Producer fenced exception thrown by Pulsar client.
     */
    public static class ProducerFencedException extends PulsarClientException {
        /**
         * Constructs a {@code ProducerFencedException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public ProducerFencedException(String msg) {
            super(msg);
        }
    }

    /**
     * Authentication exception thrown by Pulsar client.
     */
    public static class AuthenticationException extends PulsarClientException {
        /**
         * Constructs an {@code AuthenticationException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public AuthenticationException(String msg) {
            super(msg);
        }
    }

    /**
     * Authorization exception thrown by Pulsar client.
     */
    public static class AuthorizationException extends PulsarClientException {
        /**
         * Constructs an {@code AuthorizationException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public AuthorizationException(String msg) {
            super(msg);
        }
    }

    /**
     * Getting authentication data exception thrown by Pulsar client.
     */
    public static class GettingAuthenticationDataException extends PulsarClientException {
        /**
         * Constructs an {@code GettingAuthenticationDataException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public GettingAuthenticationDataException(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code GettingAuthenticationDataException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public GettingAuthenticationDataException(String msg) {
            super(msg);
        }
    }

    /**
     * Unsupported authentication exception thrown by Pulsar client.
     */
    public static class UnsupportedAuthenticationException extends PulsarClientException {
        /**
         * Constructs an {@code UnsupportedAuthenticationException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public UnsupportedAuthenticationException(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code UnsupportedAuthenticationException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public UnsupportedAuthenticationException(String msg) {
            super(msg);
        }
    }

    /**
     * Broker persistence exception thrown by Pulsar client.
     */
    public static class BrokerPersistenceException extends PulsarClientException {
        /**
         * Constructs an {@code BrokerPersistenceException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public BrokerPersistenceException(String msg) {
            super(msg);
        }
    }

    /**
     * Broker metadata exception thrown by Pulsar client.
     */
    public static class BrokerMetadataException extends PulsarClientException {
        /**
         * Constructs an {@code BrokerMetadataException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public BrokerMetadataException(String msg) {
            super(msg);
        }
    }

    /**
     * Producer busy exception thrown by Pulsar client.
     */
    public static class ProducerBusyException extends PulsarClientException {
        /**
         * Constructs an {@code ProducerBusyException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public ProducerBusyException(String msg) {
            super(msg);
        }
    }

    /**
     * Consumer busy exception thrown by Pulsar client.
     */
    public static class ConsumerBusyException extends PulsarClientException {
        /**
         * Constructs an {@code ConsumerBusyException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public ConsumerBusyException(String msg) {
            super(msg);
        }
    }

    /**
     * Not connected exception thrown by Pulsar client.
     */
    public static class NotConnectedException extends PulsarClientException {

        public NotConnectedException() {
            super("Not connected to broker");
        }

        public NotConnectedException(long sequenceId) {
            super("Not connected to broker", sequenceId);
        }

        public NotConnectedException(String msg) {
            super(msg);
        }
    }

    /**
     * Invalid message exception thrown by Pulsar client.
     */
    public static class InvalidMessageException extends PulsarClientException {
        /**
         * Constructs an {@code InvalidMessageException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public InvalidMessageException(String msg) {
            super(msg);
        }

        /**
         * Constructs an {@code InvalidMessageException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         * @param sequenceId
         *        The sequenceId of the message
         */
        public InvalidMessageException(String msg, long sequenceId) {
            super(msg, sequenceId);
        }
    }

    /**
     * Invalid topic name exception thrown by Pulsar client.
     */
    public static class InvalidTopicNameException extends PulsarClientException {
        /**
         * Constructs an {@code InvalidTopicNameException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public InvalidTopicNameException(String msg) {
            super(msg);
        }
    }

    /**
     * Not supported exception thrown by Pulsar client.
     */
    public static class NotSupportedException extends PulsarClientException {
        /**
         * Constructs an {@code NotSupportedException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public NotSupportedException(String msg) {
            super(msg);
        }
    }

    /**
     * Not supported exception thrown by Pulsar client.
     */
    public static class FeatureNotSupportedException extends NotSupportedException {

        @Getter
        private final FailedFeatureCheck failedFeatureCheck;

        public FeatureNotSupportedException(String msg, FailedFeatureCheck failedFeatureCheck) {
            super(msg);
            this.failedFeatureCheck = failedFeatureCheck;
        }
    }

    /**
     * "supports_auth_refresh" was introduced at "2.6" and is no longer supported, so skip this enum.
     * "supports_broker_entry_metadata" was introduced at "2.8" and is no longer supported, so skip this enum.
     * "supports_partial_producer" was introduced at "2.10" and is no longer supported, so skip this enum.
     * "supports_topic_watchers" was introduced at "2.11" and is no longer supported, so skip this enum.
     */
    public enum FailedFeatureCheck {
        SupportsGetPartitionedMetadataWithoutAutoCreation;
    }

    /**
     * Not allowed exception thrown by Pulsar client.
     */
    public static class NotAllowedException extends PulsarClientException {

        /**
         * Constructs an {@code NotAllowedException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public NotAllowedException(String msg) {
            super(msg);
        }
    }

    /**
     * Full producer queue error thrown by Pulsar client.
     */
    public static class ProducerQueueIsFullError extends PulsarClientException {
        /**
         * Constructs an {@code ProducerQueueIsFullError} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public ProducerQueueIsFullError(String msg) {
            super(msg);
        }

        /**
         * Constructs an {@code ProducerQueueIsFullError} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         * @param sequenceId
         *        The sequenceId of the message
         */
        public ProducerQueueIsFullError(String msg, long sequenceId) {
            super(msg, sequenceId);
        }
    }

    /**
     * Memory buffer full error thrown by Pulsar client.
     */
    public static class MemoryBufferIsFullError extends PulsarClientException {
        /**
         * Constructs an {@code MemoryBufferIsFullError} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public MemoryBufferIsFullError(String msg) {
            super(msg);
        }

        /**
         * Constructs an {@code MemoryBufferIsFullError} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         * @param sequenceId
         *        The sequenceId of the message
         */
        public MemoryBufferIsFullError(String msg, long sequenceId) {
            super(msg, sequenceId);
        }
    }

    /**
     * Producer blocked quota exceeded error thrown by Pulsar client.
     */
    public static class ProducerBlockedQuotaExceededError extends PulsarClientException {
        /**
         * Constructs an {@code ProducerBlockedQuotaExceededError} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public ProducerBlockedQuotaExceededError(String msg) {
            super(msg);
        }
    }

    /**
     * Producer blocked quota exceeded exception thrown by Pulsar client.
     */
    public static class ProducerBlockedQuotaExceededException extends PulsarClientException {
        /**
         * Constructs an {@code ProducerBlockedQuotaExceededException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public ProducerBlockedQuotaExceededException(String msg) {
            super(msg);
        }
    }

    /**
     * Checksum exception thrown by Pulsar client.
     */
    public static class ChecksumException extends PulsarClientException {
        /**
         * Constructs an {@code ChecksumException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public ChecksumException(String msg) {
            super(msg);
        }
    }

    /**
     * Crypto exception thrown by Pulsar client.
     */
    public static class CryptoException extends PulsarClientException {
        /**
         * Constructs an {@code CryptoException} with the specified detail message.
         *
         * @param msg
         *        The detail message (which is saved for later retrieval
         *        by the {@link #getMessage()} method)
         */
        public CryptoException(String msg) {
            super(msg);
        }
    }

    /**
     * Consumer assign exception thrown by Pulsar client.
     */
    public static class ConsumerAssignException extends PulsarClientException {

        /**
         * Constructs an {@code ConsumerAssignException} with the specified detail message.
         * @param msg The detail message.
         */
        public ConsumerAssignException(String msg) {
            super(msg);
        }
    }

    /**
     * Consumer assign exception thrown by Pulsar client.
     */
    public static class MessageAcknowledgeException extends PulsarClientException {

        /**
         * Constructs an {@code MessageAcknowledgeException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public MessageAcknowledgeException(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code MessageAcknowledgeException} with the specified detail message.
         * @param msg The detail message.
         */
        public MessageAcknowledgeException(String msg) {
            super(msg);
        }
    }

    /**
     * Consumer assign exception thrown by Pulsar client.
     */
    public static class TransactionConflictException extends PulsarClientException {

        /**
         * Constructs an {@code TransactionConflictException} with the specified cause.
         *
         * @param t
         *        The cause (which is saved for later retrieval by the
         *        {@link #getCause()} method).  (A null value is permitted,
         *        and indicates that the cause is nonexistent or unknown.)
         */
        public TransactionConflictException(Throwable t) {
            super(t);
        }

        /**
         * Constructs an {@code TransactionConflictException} with the specified detail message.
         * @param msg The detail message.
         */
        public TransactionConflictException(String msg) {
            super(msg);
        }
    }

    public static class TransactionHasOperationFailedException extends PulsarClientException {
        /**
         * Constructs an {@code TransactionHasOperationFailedException}.
         */
        public TransactionHasOperationFailedException() {
            super("Now allowed to commit the transaction due to failed operations of producing or acknowledgment");
        }

        /**
         * Constructs an {@code TransactionHasOperationFailedException} with the specified detail message.
         * @param msg The detail message.
         */
        public TransactionHasOperationFailedException(String msg) {
            super(msg);
        }
    }

    // wrap an exception to enriching more info messages.
    public static Throwable wrap(Throwable t, String msg) {
        if (t instanceof CompletionException) {
            return t;
        } else if (t instanceof InterruptedException) {
            return t;
        } else if (t instanceof ExecutionException) {
            return t;
        }

        try {
            String combinedMessage = msg;
            String originalMessage = t.getMessage();
            if (originalMessage != null) {
                combinedMessage += "\n" + originalMessage;
            }
            Constructor<? extends Throwable> constructor = t.getClass().getConstructor(String.class);
            Throwable newException = constructor.newInstance(combinedMessage);
            newException.setStackTrace(t.getStackTrace());
            if (t.getCause() != null) {
                newException.initCause(t.getCause());
            }
            return newException;
        } catch (Exception ignored) {
            return t;
        }
    }

    private static PulsarClientException createPulsarException(Throwable throwable) {
        PulsarClientException exception = new PulsarClientException(throwable.getMessage(), throwable);
        exception.setStackTrace(throwable.getStackTrace());
        return exception;
    }

    public static PulsarClientException unwrap(Throwable t) {
        if (t instanceof PulsarClientException) {
            return (PulsarClientException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof InterruptedException) {
            return createPulsarException(t);
        } else if (!(t instanceof ExecutionException)) {
            // Generic exception
            return createPulsarException(t);
        }

        // Unwrap the exception to keep the same exception type but a stack trace that includes the application calling
        // site
        Throwable cause = t.getCause();
        if (cause == null) {
            return createPulsarException(t);
        }
        return unwrap(cause);
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }

    public static boolean isRetriableError(Throwable t) {
        if (t instanceof AuthorizationException
                || t instanceof InvalidServiceURL
                || t instanceof InvalidConfigurationException
                || t instanceof NotFoundException
                || t instanceof IncompatibleSchemaException
                || t instanceof TopicDoesNotExistException
                || t instanceof SubscriptionNotFoundException
                || t instanceof UnsupportedAuthenticationException
                || t instanceof InvalidMessageException
                || t instanceof InvalidTopicNameException
                || t instanceof NotSupportedException
                || t instanceof NotAllowedException
                || t instanceof ChecksumException
                || t instanceof CryptoException
                || t instanceof ConsumerAssignException
                || t instanceof MessageAcknowledgeException
                || t instanceof TransactionConflictException
                || t instanceof ProducerBusyException
                || t instanceof ConsumerBusyException
                || t instanceof TransactionHasOperationFailedException) {
            return false;
        }
        return true;
    }

    public static void setPreviousExceptionCount(Throwable e, AtomicInteger previousExceptionCount) {
        if (e instanceof PulsarClientException) {
            ((PulsarClientException) e).setPreviousExceptionCount(previousExceptionCount);
            return;
        }
    }

}
