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
package org.apache.pulsar.client.api.v5;

import org.apache.pulsar.client.api.v5.async.AsyncTransaction;

/**
 * A Pulsar transaction handle.
 *
 * <p>Transactions provide exactly-once semantics across multiple topics and subscriptions.
 * Messages produced and acknowledgments made within a transaction are atomically committed
 * or aborted.
 */
public interface Transaction {

    enum State {
        OPEN,
        COMMITTING,
        ABORTING,
        COMMITTED,
        ABORTED,
        ERROR,
        TIMED_OUT
    }

    /**
     * Commit this transaction, making all produced messages visible and all acknowledgments durable.
     *
     * @throws PulsarClientException if the transaction cannot be committed (e.g., it has already
     *         been aborted, timed out, or encountered an error)
     */
    void commit() throws PulsarClientException;

    /**
     * Abort this transaction, discarding all produced messages and rolling back acknowledgments.
     *
     * @throws PulsarClientException if the transaction cannot be aborted (e.g., it has already
     *         been committed or encountered an error)
     */
    void abort() throws PulsarClientException;

    /**
     * Return an asynchronous view of this transaction.
     *
     * @return the {@link AsyncTransaction} counterpart of this transaction
     */
    AsyncTransaction async();

    /**
     * The current state of this transaction.
     *
     * @return the current {@link State} of this transaction
     */
    State state();
}
