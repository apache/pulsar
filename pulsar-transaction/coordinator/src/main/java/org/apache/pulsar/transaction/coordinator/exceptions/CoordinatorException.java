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
package org.apache.pulsar.transaction.coordinator.exceptions;

import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;

/**
 * The base exception for exceptions thrown from coordinator.
 */
public abstract class CoordinatorException extends Exception {
    private static final long serialVersionUID = 0L;

    public CoordinatorException(String message) {
        super(message);
    }

    public CoordinatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public CoordinatorException(Throwable cause) {
        super(cause);
    }

    /**
     * Exception is thrown when transaction coordinator not found.
     */
    public static class CoordinatorNotFoundException extends CoordinatorException {

        public CoordinatorNotFoundException(String msg) {
            super(msg);
        }

        public CoordinatorNotFoundException(TransactionCoordinatorID tcId) {
            super(String.format("Transaction coordinator with id %s not found!", tcId.getId()));
        }
    }

    /**
     * Exception is thrown when transaction is not in the right status.
     */
    public static class InvalidTxnStatusException extends CoordinatorException {

        private static final long serialVersionUID = 0L;

        public InvalidTxnStatusException(String message) {
            super(message);
        }

        public InvalidTxnStatusException(TxnID txnID,
                                         TxnStatus expectedStatus,
                                         TxnStatus actualStatus) {
            super("Expect Txn `" + txnID + "` to be in " + expectedStatus
                            + " status but it is in " + actualStatus + " status");

        }
    }

    /**
     * Exception is thrown when a transaction is not found in coordinator.
     */
    public static class TransactionNotFoundException extends CoordinatorException {

        private static final long serialVersionUID = 0L;

        public TransactionNotFoundException(String message) {
            super(message);
        }

        public TransactionNotFoundException(TxnID txnID) {
            super("The transaction with this txdID `" + txnID + "`not found ");
        }

        public TransactionNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }

        public TransactionNotFoundException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Exception is thrown when a operation of transaction is executed in a error transaction metadata store state.
     */
    public static class TransactionMetadataStoreStateException extends CoordinatorException {

        private static final long serialVersionUID = 0L;

        public TransactionMetadataStoreStateException(String message) {
            super(message);
        }

        public TransactionMetadataStoreStateException(TransactionCoordinatorID tcID,
                                                      TransactionMetadataStoreState.State expectedState,
                                                      TransactionMetadataStoreState.State currentState,
                                                      String operation) {
            super("Expect Transaction Coordinator `" + tcID + "` to be in " + expectedState
                    + " status but it is in " + currentState + " state for " + operation);

        }
    }
}
