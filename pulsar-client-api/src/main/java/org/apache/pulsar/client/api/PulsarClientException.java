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
import java.util.concurrent.ExecutionException;

/**
 * Base type of exception thrown by Pulsar client
 *
 *
 */
@SuppressWarnings("serial")
public class PulsarClientException extends IOException {
    public PulsarClientException(String msg) {
        super(msg);
    }

    public PulsarClientException(Throwable t) {
        super(t);
    }

    public static class InvalidServiceURL extends PulsarClientException {
        public InvalidServiceURL(Throwable t) {
            super(t);
        }
    }

    public static class InvalidConfigurationException extends PulsarClientException {
        public InvalidConfigurationException(String msg) {
            super(msg);
        }

        public InvalidConfigurationException(Throwable t) {
            super(t);
        }
    }

    public static class NotFoundException extends PulsarClientException {
        public NotFoundException(String msg) {
            super(msg);
        }

        public NotFoundException(Throwable t) {
            super(t);
        }
    }

    public static class TimeoutException extends PulsarClientException {
        public TimeoutException(Throwable t) {
            super(t);
        }

        public TimeoutException(String msg) {
            super(msg);
        }
    }

    public static class IncompatibleSchemaException extends PulsarClientException {
        public IncompatibleSchemaException(Throwable t) {
            super(t);
        }

        public IncompatibleSchemaException(String msg) {
            super(msg);
        }
    }

    public static class LookupException extends PulsarClientException {
        public LookupException(String msg) {
            super(msg);
        }
    }

    public static class TooManyRequestsException extends LookupException {
        public TooManyRequestsException(String msg) {
            super(msg);
        }
    }

    public static class ConnectException extends PulsarClientException {
        public ConnectException(Throwable t) {
            super(t);
        }

        public ConnectException(String msg) {
            super(msg);
        }
    }

    public static class AlreadyClosedException extends PulsarClientException {
        public AlreadyClosedException(String msg) {
            super(msg);
        }
    }

    public static class TopicTerminatedException extends PulsarClientException {
        public TopicTerminatedException(String msg) {
            super(msg);
        }
    }

    public static class AuthenticationException extends PulsarClientException {
        public AuthenticationException(String msg) {
            super(msg);
        }
    }

    public static class AuthorizationException extends PulsarClientException {
        public AuthorizationException(String msg) {
            super(msg);
        }
    }

    public static class GettingAuthenticationDataException extends PulsarClientException {
        public GettingAuthenticationDataException(Throwable t) {
            super(t);
        }

        public GettingAuthenticationDataException(String msg) {
            super(msg);
        }
    }

    public static class UnsupportedAuthenticationException extends PulsarClientException {
        public UnsupportedAuthenticationException(Throwable t) {
            super(t);
        }

        public UnsupportedAuthenticationException(String msg) {
            super(msg);
        }
    }

    public static class BrokerPersistenceException extends PulsarClientException {
        public BrokerPersistenceException(String msg) {
            super(msg);
        }
    }

    public static class BrokerMetadataException extends PulsarClientException {
        public BrokerMetadataException(String msg) {
            super(msg);
        }
    }

    public static class ProducerBusyException extends PulsarClientException {
        public ProducerBusyException(String msg) {
            super(msg);
        }
    }

    public static class ConsumerBusyException extends PulsarClientException {
        public ConsumerBusyException(String msg) {
            super(msg);
        }
    }

    public static class NotConnectedException extends PulsarClientException {
        public NotConnectedException() {
            super("Not connected to broker");
        }
    }

    public static class InvalidMessageException extends PulsarClientException {
        public InvalidMessageException(String msg) {
            super(msg);
        }
    }

    public static class InvalidTopicNameException extends PulsarClientException {
        public InvalidTopicNameException(String msg) {
            super(msg);
        }
    }

    public static class NotSupportedException extends PulsarClientException {
        public NotSupportedException(String msg) {
            super(msg);
        }
    }

    public static class ProducerQueueIsFullError extends PulsarClientException {
        public ProducerQueueIsFullError(String msg) {
            super(msg);
        }
    }

    public static class ProducerBlockedQuotaExceededError extends PulsarClientException {
        public ProducerBlockedQuotaExceededError(String msg) {
            super(msg);
        }
    }

    public static class ProducerBlockedQuotaExceededException extends PulsarClientException {
        public ProducerBlockedQuotaExceededException(String msg) {
            super(msg);
        }
    }

    public static class ChecksumException extends PulsarClientException {
        public ChecksumException(String msg) {
            super(msg);
        }
    }

    public static class CryptoException extends PulsarClientException {
        public CryptoException(String msg) {
            super(msg);
        }
    }

    public static PulsarClientException unwrap(Throwable t) {
        if (t instanceof PulsarClientException) {
            return (PulsarClientException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (!(t instanceof ExecutionException)) {
            // Generic exception
            return new PulsarClientException(t);
        } else if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new PulsarClientException(t);
        }

        // Unwrap the exception to keep the same exception type but a stack trace that includes the application calling
        // site
        Throwable cause = t.getCause();
        String msg = cause.getMessage();
        if (cause instanceof TimeoutException) {
            return new TimeoutException(msg);
        } else if (cause instanceof InvalidConfigurationException) {
            return new InvalidConfigurationException(msg);
        } else if (cause instanceof AuthenticationException) {
            return new AuthenticationException(msg);
        } else if (cause instanceof IncompatibleSchemaException) {
            return new IncompatibleSchemaException(msg);
        } else if (cause instanceof TooManyRequestsException) {
            return new TooManyRequestsException(msg);
        } else if (cause instanceof LookupException) {
            return new LookupException(msg);
        } else if (cause instanceof ConnectException) {
            return new ConnectException(msg);
        } else if (cause instanceof AlreadyClosedException) {
            return new AlreadyClosedException(msg);
        } else if (cause instanceof TopicTerminatedException) {
            return new TopicTerminatedException(msg);
        } else if (cause instanceof AuthorizationException) {
            return new AuthorizationException(msg);
        } else if (cause instanceof GettingAuthenticationDataException) {
            return new GettingAuthenticationDataException(msg);
        } else if (cause instanceof UnsupportedAuthenticationException) {
            return new UnsupportedAuthenticationException(msg);
        } else if (cause instanceof BrokerPersistenceException) {
            return new BrokerPersistenceException(msg);
        } else if (cause instanceof BrokerMetadataException) {
            return new BrokerMetadataException(msg);
        } else if (cause instanceof ProducerBusyException) {
            return new ProducerBusyException(msg);
        } else if (cause instanceof ConsumerBusyException) {
            return new ConsumerBusyException(msg);
        } else if (cause instanceof NotConnectedException) {
            return new NotConnectedException();
        } else if (cause instanceof InvalidMessageException) {
            return new InvalidMessageException(msg);
        } else if (cause instanceof InvalidTopicNameException) {
            return new InvalidTopicNameException(msg);
        } else if (cause instanceof NotSupportedException) {
            return new NotSupportedException(msg);
        } else if (cause instanceof ProducerQueueIsFullError) {
            return new ProducerQueueIsFullError(msg);
        } else if (cause instanceof ProducerBlockedQuotaExceededError) {
            return new ProducerBlockedQuotaExceededError(msg);
        } else if (cause instanceof ProducerBlockedQuotaExceededException) {
            return new ProducerBlockedQuotaExceededException(msg);
        } else if (cause instanceof ChecksumException) {
            return new ChecksumException(msg);
        } else if (cause instanceof CryptoException) {
            return new CryptoException(msg);
        } else {
            return new PulsarClientException(t);
        }
    }
}