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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;

/**
 * Base type of exception thrown by Pulsar Broker Service
 *
 *
 */
@SuppressWarnings("serial")
public class BrokerServiceException extends Exception {
    public BrokerServiceException(String msg) {
        super(msg);
    }

    public BrokerServiceException(Throwable t) {
        super(t);
    }

    public BrokerServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class ConsumerBusyException extends BrokerServiceException {
        public ConsumerBusyException(String msg) {
            super(msg);
        }
    }

    public static class ProducerBusyException extends BrokerServiceException {
        public ProducerBusyException(String msg) {
            super(msg);
        }
    }

    public static class ServiceUnitNotReadyException extends BrokerServiceException {
        public ServiceUnitNotReadyException(String msg) {
            super(msg);
        }
    }

    public static class TopicClosedException extends BrokerServiceException {
        public TopicClosedException(Throwable t) {
            super(t);
        }
    }

    public static class PersistenceException extends BrokerServiceException {
        public PersistenceException(Throwable t) {
            super(t);
        }
    }

    public static class TopicTerminatedException extends BrokerServiceException {
        public TopicTerminatedException(String msg) {
            super(msg);
        }

        public TopicTerminatedException(Throwable t) {
            super(t);
        }
    }

    public static class ServerMetadataException extends BrokerServiceException {
        public ServerMetadataException(Throwable t) {
            super(t);
        }

        public ServerMetadataException(String msg) {
            super(msg);
        }
    }

    public static class NamingException extends BrokerServiceException {
        public NamingException(String msg) {
            super(msg);
        }
    }

    public static class TopicFencedException extends BrokerServiceException {
        public TopicFencedException(String msg) {
            super(msg);
        }
    }

    public static class SubscriptionFencedException extends BrokerServiceException {
        public SubscriptionFencedException(String msg) {
            super(msg);
        }
    }

    public static class TopicBusyException extends BrokerServiceException {
        public TopicBusyException(String msg) {
            super(msg);
        }
    }

    public static class TopicNotFoundException extends BrokerServiceException {
        public TopicNotFoundException(String msg) {
            super(msg);
        }
    }

    public static class SubscriptionNotFoundException extends BrokerServiceException {
        public SubscriptionNotFoundException(String msg) {
            super(msg);
        }
    }

    public static class SubscriptionBusyException extends BrokerServiceException {
        public SubscriptionBusyException(String msg) {
            super(msg);
        }
    }

    public static class NotAllowedException extends BrokerServiceException {
        public NotAllowedException(String msg) {
            super(msg);
        }
    }

    public static class SubscriptionInvalidCursorPosition extends BrokerServiceException {
        public SubscriptionInvalidCursorPosition(String msg) {
            super(msg);
        }
    }

    public static class UnsupportedVersionException extends BrokerServiceException {
        public UnsupportedVersionException(String msg) {
            super(msg);
        }
    }

    public static class TooManyRequestsException extends BrokerServiceException {
        public TooManyRequestsException(String msg) {
            super(msg);
        }
    }

    public static class AlreadyRunningException extends BrokerServiceException {
        public AlreadyRunningException(String msg) {
            super(msg);
        }
    }

    public static class ConsumerAssignException extends BrokerServiceException {
        public ConsumerAssignException(String msg) {
            super(msg);
        }
    }

    public static class TopicPoliciesCacheNotInitException extends BrokerServiceException {
        public TopicPoliciesCacheNotInitException() {
            super("Topic policies cache have not init.");
        }
    }

    public static PulsarApi.ServerError getClientErrorCode(Throwable t) {
        return getClientErrorCode(t, true);
    }

    private static PulsarApi.ServerError getClientErrorCode(Throwable t, boolean checkCauseIfUnknown) {
        if (t instanceof ServerMetadataException) {
            return PulsarApi.ServerError.MetadataError;
        } else if (t instanceof NamingException) {
            return PulsarApi.ServerError.ProducerBusy;
        } else if (t instanceof PersistenceException) {
            return PulsarApi.ServerError.PersistenceError;
        } else if (t instanceof ConsumerBusyException) {
            return PulsarApi.ServerError.ConsumerBusy;
        } else if (t instanceof UnsupportedVersionException) {
            return PulsarApi.ServerError.UnsupportedVersionError;
        } else if (t instanceof TooManyRequestsException) {
            return PulsarApi.ServerError.TooManyRequests;
        } else if (t instanceof TopicTerminatedException) {
            return PulsarApi.ServerError.TopicTerminatedError;
        } else if (t instanceof ServiceUnitNotReadyException || t instanceof TopicFencedException
                || t instanceof SubscriptionFencedException) {
            return PulsarApi.ServerError.ServiceNotReady;
        } else if (t instanceof TopicNotFoundException) {
            return PulsarApi.ServerError.TopicNotFound;
        } else if (t instanceof IncompatibleSchemaException
            || t instanceof InvalidSchemaDataException) {
            // for backward compatible with old clients, invalid schema data
            // is treated as "incompatible schema".
            return PulsarApi.ServerError.IncompatibleSchema;
        } else if (t instanceof ConsumerAssignException) {
            return ServerError.ConsumerAssignError;
        } else if (t instanceof CoordinatorException.CoordinatorNotFoundException) {
            return ServerError.TransactionCoordinatorNotFound;
        } else if (t instanceof CoordinatorException.InvalidTxnStatusException) {
            return ServerError.InvalidTxnStatus;
        } else if (t instanceof NotAllowedException) {
            return ServerError.NotAllowedError;
        } else {
            if (checkCauseIfUnknown) {
                return getClientErrorCode(t.getCause(), false);
            } else {
                return PulsarApi.ServerError.UnknownError;
            }
        }
    }
}
