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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * The class represents a transaction within Pulsar.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Transaction {

    enum State {

        /**
         * When a transaction is in the `OPEN` state, messages can be produced and acked with this transaction.
         *
         * When a transaction is in the `OPEN` state, it can commit or abort.
         */
        OPEN,

        /**
         * When a client invokes a commit, the transaction state is changed from `OPEN` to `COMMITTING`.
         */
        COMMITTING,

        /**
         * When a client invokes an abort, the transaction state is changed from `OPEN` to `ABORTING`.
         */
        ABORTING,

        /**
         * When a client receives a response to a commit, the transaction state is changed from
         * `COMMITTING` to `COMMITTED`.
         */
        COMMITTED,

        /**
         * When a client receives a response to an abort, the transaction state is changed from `ABORTING` to `ABORTED`.
         */
        ABORTED,

        /**
         * When a client invokes a commit or an abort, but a transaction does not exist in a coordinator,
         * then the state is changed to `ERROR`.
         *
         * When a client invokes a commit, but the transaction state in a coordinator is `ABORTED` or `ABORTING`,
         * then the state is changed to `ERROR`.
         *
         * When a client invokes an abort, but the transaction state in a coordinator is `COMMITTED` or `COMMITTING`,
         * then the state is changed to `ERROR`.
         */
        ERROR,

        /**
         * When a transaction is timed out and the transaction state is `OPEN`,
         * then the transaction state is changed from `OPEN` to `TIME_OUT`.
         */
        TIME_OUT
    }

    /**
     * Commit the transaction.
     *
     * @return the future represents the commit result.
     */
    CompletableFuture<Void> commit();

    /**
     * Abort the transaction.
     *
     * @return the future represents the abort result.
     */
    CompletableFuture<Void> abort();

    /**
     * Get TxnID of the transaction.
     *  @return {@link TxnID} the txnID.
     */
    TxnID getTxnID();

    /**
     * Get transaction state.
     *
     * @return {@link State} the state of the transaction.
     */
    State getState();

}
