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
package org.apache.pulsar.broker.service;

import lombok.Getter;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;

/**
 * Base type of exception thrown by Pulsar Broker Service.
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

    public static class ProducerFencedException extends BrokerServiceException {
        public ProducerFencedException(String msg) {
            super(msg);
        }
    }

    public static class ServiceUnitNotReadyException extends BrokerServiceException {
        public ServiceUnitNotReadyException(String msg) {
            super(msg);
        }

        public ServiceUnitNotReadyException(String msg, Throwable t) {
            super(msg, t);
        }
    }

    public static class TopicClosedException extends BrokerServiceException {
        public TopicClosedException(Throwable t) {
            super(t);
        }
    }

    @Deprecated
    public static class AddEntryMetadataException extends BrokerServiceException {
        public AddEntryMetadataException(Throwable t) {
            super(t);
        }
    }

    public static class PersistenceException extends BrokerServiceException {
        public PersistenceException(Throwable t) {
            super(t);
        }

        public PersistenceException(String msg) {
            super(msg);
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

    public static class TopicMigratedException extends BrokerServiceException {
        public TopicMigratedException(String msg) {
            super(msg);
        }

        public TopicMigratedException(Throwable t) {
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

        public TopicBusyException(String msg, Throwable t) {
            super(msg, t);
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

    public static class UnsupportedSubscriptionException extends BrokerServiceException {
        public UnsupportedSubscriptionException(String msg) {
            super(msg);
        }
    }

    public static class SubscriptionConflictUnloadException extends BrokerServiceException {
        public SubscriptionConflictUnloadException(String msg) {
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

    public static class ConnectionClosedException extends BrokerServiceException {
        public ConnectionClosedException(String msg) {
            super(msg);
        }
    }

    public static class TopicBacklogQuotaExceededException extends BrokerServiceException {
        @Getter
        private final BacklogQuota.RetentionPolicy retentionPolicy;

        public TopicBacklogQuotaExceededException(BacklogQuota.RetentionPolicy retentionPolicy) {
            super("Cannot create producer on topic with backlog quota exceeded");
            this.retentionPolicy = retentionPolicy;
        }
    }

    public static org.apache.pulsar.common.api.proto.ServerError getClientErrorCode(Throwable t) {
        return getClientErrorCode(t, true);
    }

    private static ServerError getClientErrorCode(Throwable t, boolean checkCauseIfUnknown) {
        if (t instanceof ServerMetadataException) {
            return ServerError.MetadataError;
        } else if (t instanceof NamingException) {
            return ServerError.ProducerBusy;
        } else if (t instanceof PersistenceException) {
            return ServerError.PersistenceError;
        } else if (t instanceof ConsumerBusyException) {
            return ServerError.ConsumerBusy;
        } else if (t instanceof SubscriptionBusyException) {
            return ServerError.ConsumerBusy;
        } else if (t instanceof ProducerBusyException) {
            return ServerError.ProducerBusy;
        } else if (t instanceof UnsupportedVersionException) {
            return ServerError.UnsupportedVersionError;
        } else if (t instanceof TooManyRequestsException) {
            return ServerError.TooManyRequests;
        } else if (t instanceof TopicTerminatedException) {
            return ServerError.TopicTerminatedError;
        } else if (t instanceof ServiceUnitNotReadyException || t instanceof TopicFencedException
                || t instanceof SubscriptionFencedException) {
            return ServerError.ServiceNotReady;
        } else if (t instanceof TopicNotFoundException) {
            return ServerError.TopicNotFound;
        } else if (t instanceof SubscriptionNotFoundException) {
            return ServerError.SubscriptionNotFound;
        } else if (t instanceof IncompatibleSchemaException
            || t instanceof InvalidSchemaDataException) {
            // for backward compatible with old clients, invalid schema data
            // is treated as "incompatible schema".
            return ServerError.IncompatibleSchema;
        } else if (t instanceof ConsumerAssignException) {
            return ServerError.ConsumerAssignError;
        } else if (t instanceof CoordinatorException.CoordinatorNotFoundException) {
            return ServerError.TransactionCoordinatorNotFound;
        } else if (t instanceof CoordinatorException.InvalidTxnStatusException) {
            return ServerError.InvalidTxnStatus;
        } else if (t instanceof NotAllowedException) {
            return ServerError.NotAllowedError;
        } else if (t instanceof ProducerFencedException) {
            return ServerError.ProducerFenced;
        } else if (t instanceof TransactionConflictException) {
            return ServerError.TransactionConflict;
        } else if (t instanceof CoordinatorException.TransactionNotFoundException) {
            return ServerError.TransactionNotFound;
        } else {
            if (checkCauseIfUnknown) {
                return getClientErrorCode(t.getCause(), false);
            } else {
                return ServerError.UnknownError;
            }
        }
    }
}
