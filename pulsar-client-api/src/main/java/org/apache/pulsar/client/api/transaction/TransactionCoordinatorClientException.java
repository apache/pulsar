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
package org.apache.pulsar.client.api.transaction;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Exceptions for transaction coordinator client.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TransactionCoordinatorClientException extends IOException {

    public TransactionCoordinatorClientException(Throwable t) {
        super(t);
    }

    public TransactionCoordinatorClientException(String message) {
        super(message);
    }

    /**
     * Thrown when transaction coordinator with unexpected state.
     */
    public static class CoordinatorClientStateException extends TransactionCoordinatorClientException {

        public CoordinatorClientStateException() {
            super("Unexpected state for transaction metadata client.");
        }

        public CoordinatorClientStateException(String message) {
            super(message);
        }
    }

    /**
     * Thrown when transaction coordinator not found in broker side.
     */
    public static class CoordinatorNotFoundException extends TransactionCoordinatorClientException {
        public CoordinatorNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Thrown when transaction switch to a invalid status.
     */
    public static class InvalidTxnStatusException extends TransactionCoordinatorClientException {
        public InvalidTxnStatusException(String message) {
            super(message);
        }

        public InvalidTxnStatusException(String txnId, String actualState, String expectState) {
            super("[" + txnId + "] with unexpected state : "
                    + actualState + ", expect " + expectState + " state!");
        }
    }

    /**
     * Thrown when transaction not found in transaction coordinator.
     */
    public static class TransactionNotFoundException extends TransactionCoordinatorClientException {
        public TransactionNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Thrown when transaction meta store handler not exists.
     */
    public static class MetaStoreHandlerNotExistsException extends TransactionCoordinatorClientException {

        public MetaStoreHandlerNotExistsException(long tcId) {
            super("Transaction meta store handler for transaction meta store {} not exists.");
        }

        public MetaStoreHandlerNotExistsException(String message) {
            super(message);
        }
    }

    /**
     * Thrown when send request to transaction meta store but the transaction meta store handler not ready.
     */
    public static class MetaStoreHandlerNotReadyException extends TransactionCoordinatorClientException {
        public MetaStoreHandlerNotReadyException(long tcId) {
            super("Transaction meta store handler for transaction meta store {} not ready now.");
        }

        public MetaStoreHandlerNotReadyException(String message) {
            super(message);
        }
    }

    public static TransactionCoordinatorClientException unwrap(Throwable t) {
        if (t instanceof TransactionCoordinatorClientException) {
            return (TransactionCoordinatorClientException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new TransactionCoordinatorClientException(t);
        } else if (t instanceof CoordinatorNotFoundException) {
            return (CoordinatorNotFoundException) t;
        } else if (t instanceof InvalidTxnStatusException) {
            return (InvalidTxnStatusException) t;
        } else if (t instanceof ExecutionException | t instanceof CompletionException) {
            // Generic exception
            return unwrap(t.getCause());
        } else {
            return new TransactionCoordinatorClientException(t);
        }

    }
}
