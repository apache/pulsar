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
package org.apache.pulsar.transaction.buffer;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.transaction.impl.common.TxnID;

/**
 * A client that communicates with others.
 */
public interface TransactionBufferClient {

    /**
     * Check the specified transaction exists or not.
     *
     * @param txnID the specified transaction
     * @return
     */
    boolean exist(TxnID txnID);

    /**
     * Asynchronous check the specified transaction exists or not.
     *
     * @param txnID the specified transaction
     * @return
     */
    CompletableFuture<Void> asyncExist(TxnID txnID);

    /**
     * Create a new transaction meta.
     *
     * @param txnID the transaction id
     * @return
     */
    TransactionMeta create(TxnID txnID);

    /**
     * Asynchronous create a new transaction meta.
     *
     * @param txnID the transaction id
     * @return
     */
    CompletableFuture<TransactionMeta> asyncCreate(TxnID txnID);

    /**
     * Check the specified transaction is open.
     *
     * @param txnID the specified transaction
     * @return
     */
    boolean isOpen(TxnID txnID);

    /**
     * Asynchronous check the specified transaction is open.
     *
     * @param txnID the specified transaction
     * @return
     */
    CompletableFuture<Void> asyncIsOpen(TxnID txnID);

    /**
     * Check the specified transaction is committing.
     *
     * @param txnID the specified transaction
     * @return
     */
    boolean isCommitting(TxnID txnID);

    /**
     * Asynchronous check the specified transaction is committing.
     *
     * @param txnID the specified transaction
     * @return
     */
    CompletableFuture<Void> asyncIsCommitting(TxnID txnID);

    /**
     * Check the specified transaction is committed.
     *
     * @param txnID the specified transaction
     * @return
     */
    boolean isCommitted(TxnID txnID);

    /**
     * Asynchronous check the specified transaction is committed.
     *
     * @param txnID the specified transaction
     * @return
     */
    CompletableFuture<Void> asyncIsCommitted(TxnID txnID);

    /**
     * Check the specified transaction is aborting.
     *
     * @param txnID the specified transaction
     * @return
     */
    boolean isAborting(TxnID txnID);

    /**
     * Asynchronous check the specified transaction is aborting.
     *
     * @param txnID the specified transaction
     * @return
     */
    CompletableFuture<Void> asyncIsAborting(TxnID txnID);

    /**
     * Check the specified transaction is aborted.
     *
     * @param txnID the specified transaction
     * @return
     */
    boolean isAborted(TxnID txnID);

    /**
     * Asynchronous check the specified transaction is aborted.
     *
     * @param txnID the specified transaction
     * @return
     */
    CompletableFuture<Void> asyncIsAborted(TxnID txnID);

    /**
     * Commit the specified transaction.
     *
     * @param txnID the commit transaction id
     * @param committedLedgerId the data ledger id which the commit marker at
     * @param committedEntryId the data entry id which the commit marker at
     * @return
     */
    boolean commitTxn(TxnID txnID, long committedLedgerId, long committedEntryId);

    /**
     * Asynchronous commit the specified transaction.
     *
     * @param txnID the commit transaction id
     * @param committedLedgerId the data ledger id which the commit marker at
     * @param commttedEntryId the data entry id which the commit marker at
     * @return
     */
    CompletableFuture<Void> asyncCommitTxn(TxnID txnID, long committedLedgerId, long commttedEntryId);

    /**
     * Abort the specified transaction.
     *
     * @param txnID the aborted transaction id
     * @return
     */
    boolean abortTxn(TxnID txnID);

    /**
     * Asynchronous abort the specified transaction.
     *
     * @param txnID the aborted transaction id
     * @return
     */
    CompletableFuture<Void> asyncAbortTxn(TxnID txnID);

    /**
     * Append the message to the specified transaction.
     *
     * @param txnID the transaction id
     * @param messagePayload the message
     * @return
     */
    boolean append(TxnID txnID, ByteBuf messagePayload);

    /**
     * Asynchronous append the message to the specified transaction.
     *
     * @param txnID the transaction id
     * @param messagePayload the message
     * @return
     */
    CompletableFuture<Void> asyncAppend(TxnID txnID, ByteBuf messagePayload);

    /**
     * Get all messages in the specified transaction.
     *
     * @param txnID the transaction id
     * @return
     */
    List<ByteBuf> getTxnMessages(TxnID txnID);

    /**
     * Asynchronous get all messages in the specified transaction.
     *
     * @param txnID the transaction id
     * @return
     */
    CompletableFuture<List<ByteBuf>> asyncGetTxnMessages(TxnID txnID);
}
