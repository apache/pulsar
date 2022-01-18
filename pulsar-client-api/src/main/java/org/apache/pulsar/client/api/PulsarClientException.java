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
package org.apache.pulsar.client.api;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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
    private Collection<Throwable> previous;

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

    /**
     * Add a list of previous exception which occurred for the same operation
     * and have been retried.
     *
     * @param previous A collection of throwables that triggered retries
     */
    public void setPreviousExceptions(Collection<Throwable> previous) {
        this.previous = previous;
    }

    /**
     * Get the collection of previous exceptions which have caused retries
     * for this operation.
     *
     * @return a collection of exception, ordered as they occurred
     */
    public Collection<Throwable> getPreviousExceptions() {
        return this.previous;
    }

    @Override
    public String toString() {
        if (previous == null || previous.isEmpty()) {
            return super.toString();
        } else {
            StringBuilder sb = new StringBuilder(super.toString());
            int i = 0;
            boolean first = true;
            sb.append("{\"previous\":[");
            for (Throwable t : previous) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                sb.append("{\"attempt\":").append(i++)
                    .append(",\"error\":\"").append(t.toString().replace("\"", "\\\""))
                    .append("\"}");
            }
            sb.append("]}");
            return sb.toString();
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

    // wrap an exception to enriching more info messages.
    public static Throwable wrap(Throwable t, String msg) {
        msg += "\n" + t.getMessage();
        // wrap an exception with new message info
        if (t instanceof TimeoutException) {
            return new TimeoutException(msg);
        } else if (t instanceof InvalidConfigurationException) {
            return new InvalidConfigurationException(msg);
        } else if (t instanceof AuthenticationException) {
            return new AuthenticationException(msg);
        } else if (t instanceof IncompatibleSchemaException) {
            return new IncompatibleSchemaException(msg);
        } else if (t instanceof TooManyRequestsException) {
            return new TooManyRequestsException(msg);
        } else if (t instanceof LookupException) {
            return new LookupException(msg);
        } else if (t instanceof ConnectException) {
            return new ConnectException(msg);
        } else if (t instanceof AlreadyClosedException) {
            return new AlreadyClosedException(msg);
        } else if (t instanceof TopicTerminatedException) {
            return new TopicTerminatedException(msg);
        } else if (t instanceof AuthorizationException) {
            return new AuthorizationException(msg);
        } else if (t instanceof GettingAuthenticationDataException) {
            return new GettingAuthenticationDataException(msg);
        } else if (t instanceof UnsupportedAuthenticationException) {
            return new UnsupportedAuthenticationException(msg);
        } else if (t instanceof BrokerPersistenceException) {
            return new BrokerPersistenceException(msg);
        } else if (t instanceof BrokerMetadataException) {
            return new BrokerMetadataException(msg);
        } else if (t instanceof ProducerBusyException) {
            return new ProducerBusyException(msg);
        } else if (t instanceof ConsumerBusyException) {
            return new ConsumerBusyException(msg);
        } else if (t instanceof NotConnectedException) {
            return new NotConnectedException();
        } else if (t instanceof InvalidMessageException) {
            return new InvalidMessageException(msg);
        } else if (t instanceof InvalidTopicNameException) {
            return new InvalidTopicNameException(msg);
        } else if (t instanceof NotSupportedException) {
            return new NotSupportedException(msg);
        } else if (t instanceof NotAllowedException) {
            return new NotAllowedException(msg);
        } else if (t instanceof ProducerQueueIsFullError) {
            return new ProducerQueueIsFullError(msg);
        } else if (t instanceof ProducerBlockedQuotaExceededError) {
            return new ProducerBlockedQuotaExceededError(msg);
        } else if (t instanceof ProducerBlockedQuotaExceededException) {
            return new ProducerBlockedQuotaExceededException(msg);
        } else if (t instanceof ChecksumException) {
            return new ChecksumException(msg);
        } else if (t instanceof CryptoException) {
            return new CryptoException(msg);
        } else if (t instanceof ConsumerAssignException) {
            return new ConsumerAssignException(msg);
        } else if (t instanceof MessageAcknowledgeException) {
            return new MessageAcknowledgeException(msg);
        } else if (t instanceof TransactionConflictException) {
            return new TransactionConflictException(msg);
        } else if (t instanceof PulsarClientException) {
            return new PulsarClientException(msg);
        } else if (t instanceof CompletionException) {
            return t;
        } else if (t instanceof RuntimeException) {
            return new RuntimeException(msg, t.getCause());
        } else if (t instanceof InterruptedException) {
            return t;
        } else if (t instanceof ExecutionException) {
            return t;
        }

        return t;
    }

    public static PulsarClientException unwrap(Throwable t) {
        if (t instanceof PulsarClientException) {
            return (PulsarClientException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }  else if (t instanceof InterruptedException) {
            return new PulsarClientException(t);
        } else if (!(t instanceof ExecutionException)) {
            // Generic exception
            return new PulsarClientException(t);
        }

        // Unwrap the exception to keep the same exception type but a stack trace that includes the application calling
        // site
        Throwable cause = t.getCause();
        String msg = cause.getMessage();
        PulsarClientException newException;
        if (cause instanceof TimeoutException) {
            newException = new TimeoutException(msg);
        } else if (cause instanceof InvalidConfigurationException) {
            newException = new InvalidConfigurationException(msg);
        } else if (cause instanceof AuthenticationException) {
            newException = new AuthenticationException(msg);
        } else if (cause instanceof IncompatibleSchemaException) {
            newException = new IncompatibleSchemaException(msg);
        } else if (cause instanceof TooManyRequestsException) {
            newException = new TooManyRequestsException(msg);
        } else if (cause instanceof LookupException) {
            newException = new LookupException(msg);
        } else if (cause instanceof ConnectException) {
            newException = new ConnectException(msg);
        } else if (cause instanceof AlreadyClosedException) {
            newException = new AlreadyClosedException(msg);
        } else if (cause instanceof TopicTerminatedException) {
            newException = new TopicTerminatedException(msg);
        } else if (cause instanceof AuthorizationException) {
            newException = new AuthorizationException(msg);
        } else if (cause instanceof GettingAuthenticationDataException) {
            newException = new GettingAuthenticationDataException(msg);
        } else if (cause instanceof UnsupportedAuthenticationException) {
            newException = new UnsupportedAuthenticationException(msg);
        } else if (cause instanceof BrokerPersistenceException) {
            newException = new BrokerPersistenceException(msg);
        } else if (cause instanceof BrokerMetadataException) {
            newException = new BrokerMetadataException(msg);
        } else if (cause instanceof ProducerBusyException) {
            newException = new ProducerBusyException(msg);
        } else if (cause instanceof ConsumerBusyException) {
            newException = new ConsumerBusyException(msg);
        } else if (cause instanceof NotConnectedException) {
            newException = new NotConnectedException();
        } else if (cause instanceof InvalidMessageException) {
            newException = new InvalidMessageException(msg);
        } else if (cause instanceof InvalidTopicNameException) {
            newException = new InvalidTopicNameException(msg);
        } else if (cause instanceof NotSupportedException) {
            newException = new NotSupportedException(msg);
        } else if (cause instanceof NotAllowedException) {
            newException = new NotAllowedException(msg);
        } else if (cause instanceof ProducerQueueIsFullError) {
            newException = new ProducerQueueIsFullError(msg);
        } else if (cause instanceof ProducerBlockedQuotaExceededError) {
            newException = new ProducerBlockedQuotaExceededError(msg);
        } else if (cause instanceof ProducerBlockedQuotaExceededException) {
            newException = new ProducerBlockedQuotaExceededException(msg);
        } else if (cause instanceof ChecksumException) {
            newException = new ChecksumException(msg);
        } else if (cause instanceof CryptoException) {
            newException = new CryptoException(msg);
        } else if (cause instanceof ConsumerAssignException) {
            newException = new ConsumerAssignException(msg);
        } else if (cause instanceof MessageAcknowledgeException) {
            newException = new MessageAcknowledgeException(msg);
        } else if (cause instanceof TransactionConflictException) {
            newException = new TransactionConflictException(msg);
        } else if (cause instanceof TopicDoesNotExistException) {
            newException = new TopicDoesNotExistException(msg);
        } else if (cause instanceof ProducerFencedException) {
            newException = new ProducerFencedException(msg);
        } else if (cause instanceof MemoryBufferIsFullError) {
            newException = new MemoryBufferIsFullError(msg);
        } else if (cause instanceof NotFoundException) {
            newException = new NotFoundException(msg);
        } else {
            newException = new PulsarClientException(t);
        }

        Collection<Throwable> previousExceptions = getPreviousExceptions(t);
        if (previousExceptions != null) {
            newException.setPreviousExceptions(previousExceptions);
        }
        return newException;
    }

    public static Collection<Throwable> getPreviousExceptions(Throwable t) {
        Throwable e = t;
        for (int maxDepth = 20; maxDepth > 0 && e != null; maxDepth--) {
            if (e instanceof PulsarClientException) {
                Collection<Throwable> previous = ((PulsarClientException) e).getPreviousExceptions();
                if (previous != null) {
                    return previous;
                }
            }
            e = t.getCause();
        }
        return null;
    }

    public static void setPreviousExceptions(Throwable t, Collection<Throwable> previous) {
        Throwable e = t;
        for (int maxDepth = 20; maxDepth > 0 && e != null; maxDepth--) {
            if (e instanceof PulsarClientException) {
                ((PulsarClientException) e).setPreviousExceptions(previous);
                return;
            }
            e = t.getCause();
        }
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
                || t instanceof ConsumerBusyException) {
            return false;
        }
        return true;
    }
}
