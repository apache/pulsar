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
 *
 */
package org.apache.pulsar.client.api;

import java.util.concurrent.CompletableFuture;

public interface Transaction {

    /**
     * Get the transaction id.
     *
     * @return
     */
    TxnId getTxnId();

    /**
     * Check the specified transaction exists or not.
     *
     * @return
     */
    boolean exist();

    /**
     * Asynchronous check the specified transaction exists or not.
     *
     * @return
     */
    CompletableFuture<Void> asyncExist();

    /**
     * Check the specified transaction is open.
     *
     * @return
     */
    boolean isOpen();

    /**
     * Asynchronous check the specified transaction is open.
     *
     * @return
     */
    CompletableFuture<Void> asyncIsOpen();

    /**
     * Check the specified transaction is committing.
     *
     * @return
     */
    boolean isCommitting();

    /**
     * Asynchronous check the specified transaction is committing.
     *
     * @return
     */
    CompletableFuture<Void> asyncIsCommitting();

    /**
     * Check the specified transaction is committed.
     *
     * @return
     */
    boolean isCommitted();

    /**
     * Asynchronous check the specified transaction is committed.
     *
     * @return
     */
    CompletableFuture<Void> asyncIsCommitted();

    /**
     * Check the specified transaction is aborting.
     *
     * @return
     */
    boolean isAborting();

    /**
     * Asynchronous check the specified transaction is aborting.
     *
     * @return
     */
    CompletableFuture<Void> asyncIsAborting();

    /**
     * Check the specified transaction is aborted.
     *
     * @return
     */
    boolean isAborted();

    /**
     * Asynchronous check the specified transaction is aborted.
     *
     * @return
     */
    CompletableFuture<Void> asyncIsAborted();

    /**
     * Commit the specified transaction.
     *
     * @return
     */
    boolean commitTxn();

    /**
     * Asynchronous commit the specified transaction.
     *
     * @return
     */
    CompletableFuture<Void> asyncCommitTxn();

    /**
     * Abort the specified transaction.
     *
     * @return
     */
    boolean abortTxn();

    /**
     * Asynchronous abort the specified transaction.
     *
     * @return
     */
    CompletableFuture<Void> asyncAbortTxn();
}
