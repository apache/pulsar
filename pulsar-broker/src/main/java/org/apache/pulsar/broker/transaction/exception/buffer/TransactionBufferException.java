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
package org.apache.pulsar.broker.transaction.exception.buffer;

import org.apache.pulsar.broker.transaction.exception.TransactionException;

/**
 * The base exception class for the errors thrown from Transaction Buffer.
 */
public abstract class TransactionBufferException extends TransactionException {

    private static final long serialVersionUID = 0L;

    public TransactionBufferException(String message) {
        super(message);
    }

    public TransactionBufferException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionBufferException(Throwable cause) {
        super(cause);
    }


    /**
     * Exception thrown when reaching end of a transaction.
     */
    public static class EndOfTransactionException extends TransactionBufferException {

        private static final long serialVersionUID = 0L;

        public EndOfTransactionException(String message) {
            super(message);
        }
    }

    /**
     * Exception is thrown when no transactions found committed at a given ledger.
     */
    public class NoTxnsCommittedAtLedgerException extends TransactionBufferException {

        private static final long serialVersionUID = 0L;

        public NoTxnsCommittedAtLedgerException(String message) {
            super(message);
        }
    }

    /**
     * Transaction buffer provider exception.
     */
    public class TransactionBufferProviderException extends TransactionBufferException {

        public TransactionBufferProviderException(String message) {
            super(message);
        }

    }

    /**
     * Exception is thrown when the transaction is not found in the transaction buffer.
     */
    public static class TransactionNotFoundException extends TransactionBufferException {

        private static final long serialVersionUID = 0L;

        public TransactionNotFoundException(String message) {
            super(message);
        }
    }


}
