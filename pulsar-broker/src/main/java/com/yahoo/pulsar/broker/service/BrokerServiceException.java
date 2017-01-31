/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service;

import com.yahoo.pulsar.common.api.proto.PulsarApi;

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

    public static class ConsumerBusyException extends BrokerServiceException {
        public ConsumerBusyException(String msg) {
            super(msg);
        }
    }

    public static class ServiceUnitNotReadyException extends BrokerServiceException {
        public ServiceUnitNotReadyException(String msg) {
            super(msg);
        }
    }

    public static class PersistenceException extends BrokerServiceException {
        public PersistenceException(Throwable t) {
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

    public static PulsarApi.ServerError getClientErrorCode(Throwable t) {
        if (t instanceof ServerMetadataException) {
            return PulsarApi.ServerError.MetadataError;
        } else if (t instanceof PersistenceException) {
            return PulsarApi.ServerError.PersistenceError;
        } else if (t instanceof ConsumerBusyException) {
            return PulsarApi.ServerError.ConsumerBusy;
        } else if (t instanceof UnsupportedVersionException) {
            return PulsarApi.ServerError.UnsupportedVersionError;
        } else if (t instanceof ServiceUnitNotReadyException || t instanceof TopicFencedException
                || t instanceof SubscriptionFencedException) {
            return PulsarApi.ServerError.ServiceNotReady;
        } else {
            return PulsarApi.ServerError.UnknownError;
        }
    }
}