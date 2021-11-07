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
package org.apache.pulsar.broker.transaction.exception;

import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;

/**
 * The base exception class for the errors thrown from Transaction.
 */
public abstract class TransactionException extends Exception {

    private static final long serialVersionUID = 0L;

    public TransactionException(String message) {
        super(message);
    }

    public TransactionException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionException(Throwable cause) {
        super(cause);
    }

    /**
     * Exception is thrown when opening a reader on a transaction that is not sealed yet.
     */
    public static class TransactionNotSealedException extends TransactionException {

        private static final long serialVersionUID = 0L;

        public TransactionNotSealedException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown if a transaction is already sealed.
     *
     * <p>If a transaction is sealed, no more entries should be appended to this transaction.
     */
    public static class TransactionSealedException extends TransactionException {

        private static final long serialVersionUID = 5366602873819540477L;

        public TransactionSealedException(String message) {
            super(message);
        }
    }

    /**
     * Exceptions are thrown when operations are applied to a transaction which is not in expected txn status.
     */
    public static class TransactionStatusException extends TransactionException {

        private static final long serialVersionUID = 0L;

        public TransactionStatusException(TxnID txnId,
                                          TxnStatus expectedStatus,
                                          TxnStatus actualStatus) {
            super("Transaction `q" + txnId + "` is not in an expected status `" + expectedStatus
                    + "`, but is in status `" + actualStatus + "`");
        }
    }
}
