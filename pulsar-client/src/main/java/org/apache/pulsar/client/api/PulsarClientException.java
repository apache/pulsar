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

    public static class TimeoutException extends PulsarClientException {
        public TimeoutException(String msg) {
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
}