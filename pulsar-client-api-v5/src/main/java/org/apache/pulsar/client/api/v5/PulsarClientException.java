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
package org.apache.pulsar.client.api.v5;

import java.io.IOException;

/**
 * Base exception for all Pulsar client operations.
 */
public class PulsarClientException extends IOException {

    public PulsarClientException(String message) {
        super(message);
    }

    public PulsarClientException(Throwable cause) {
        super(cause);
    }

    public PulsarClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class InvalidServiceURLException extends PulsarClientException {
        public InvalidServiceURLException(String message) {
            super(message);
        }

        public InvalidServiceURLException(Throwable cause) {
            super(cause);
        }
    }

    public static final class InvalidConfigurationException extends PulsarClientException {
        public InvalidConfigurationException(String message) {
            super(message);
        }

        public InvalidConfigurationException(Throwable cause) {
            super(cause);
        }
    }

    public static final class NotFoundException extends PulsarClientException {
        public NotFoundException(String message) {
            super(message);
        }
    }

    public static final class TimeoutException extends PulsarClientException {
        public TimeoutException(String message) {
            super(message);
        }

        public TimeoutException(Throwable cause) {
            super(cause);
        }
    }

    public static final class AlreadyClosedException extends PulsarClientException {
        public AlreadyClosedException(String message) {
            super(message);
        }
    }

    public static final class AuthenticationException extends PulsarClientException {
        public AuthenticationException(String message) {
            super(message);
        }
    }

    public static final class AuthorizationException extends PulsarClientException {
        public AuthorizationException(String message) {
            super(message);
        }
    }

    public static final class ConnectException extends PulsarClientException {
        public ConnectException(String message) {
            super(message);
        }

        public ConnectException(Throwable cause) {
            super(cause);
        }
    }

    public static final class ProducerBusyException extends PulsarClientException {
        public ProducerBusyException(String message) {
            super(message);
        }
    }

    public static final class ConsumerBusyException extends PulsarClientException {
        public ConsumerBusyException(String message) {
            super(message);
        }
    }

    public static final class ProducerQueueIsFullException extends PulsarClientException {
        public ProducerQueueIsFullException(String message) {
            super(message);
        }
    }

    public static final class IncompatibleSchemaException extends PulsarClientException {
        public IncompatibleSchemaException(String message) {
            super(message);
        }
    }

    public static final class TopicTerminatedException extends PulsarClientException {
        public TopicTerminatedException(String message) {
            super(message);
        }
    }

    public static final class CryptoException extends PulsarClientException {
        public CryptoException(String message) {
            super(message);
        }
    }

    public static final class TransactionConflictException extends PulsarClientException {
        public TransactionConflictException(String message) {
            super(message);
        }
    }

    public static final class NotConnectedException extends PulsarClientException {
        public NotConnectedException() {
            super("Not connected");
        }

        public NotConnectedException(String message) {
            super(message);
        }
    }

    public static final class MemoryBufferIsFullException extends PulsarClientException {
        public MemoryBufferIsFullException(String message) {
            super(message);
        }
    }
}
