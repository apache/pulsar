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
package org.apache.pulsar.broker.transaction.exception.coordinator;

import org.apache.pulsar.broker.transaction.exception.TransactionException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;

/**
 * The base exception class for the errors thrown from Transaction Coordinator.
 */
public abstract class TransactionCoordinatorException extends TransactionException {

    private static final long serialVersionUID = 0L;

    public TransactionCoordinatorException(String message) {
        super(message);
    }

    public TransactionCoordinatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionCoordinatorException(Throwable cause) {
        super(cause);
    }


    /**
     * Exceptions are thrown when txnAction is unsupported.
     */
    public static class UnsupportedTxnActionException extends TransactionCoordinatorException {

        private static final long serialVersionUID = 0L;

        public UnsupportedTxnActionException(TxnID txnId, int txnAction) {
            super("Transaction `" + txnId + "` receive unsupported txnAction " + TxnAction.valueOf(txnAction));
        }
    }
}
