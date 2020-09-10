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
package org.apache.pulsar.broker.transaction.buffer;

import com.google.common.annotations.Beta;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * A class represent a transaction buffer. The transaction buffer
 * is per partition. All the messages published within transactions
 * are appended to a transaction buffer. They are not visible to consumers
 * or readers before the transaction is committed.
 *
 * <p>When committing transaction starts, the broker will append a `COMMITTED`
 * marker to the data partition first to mark the transaction is committed.
 * The broker knows the data ledger of the commit marker and calls {@link #commitTxn(TxnID, long, long)}
 * to commit and seal the buffer.
 *
 * <p>When the marker is appended to the data partition, all the entries are visible
 * to the consumers. So a transaction reader {@link TransactionBufferReader} will be
 * opened to read the entries when the broker sees the commit marker. There is a chance
 * broker crashes after writing the marker to data partition but before committing
 * the transaction in transaction buffer. That is fine. Because the transaction buffer
 * will fail opening the transaction buffer reader since the transaction is still marked
 * as open. The broker can keep retry until the TC (transaction coordinator) eventually
 * commits the buffer again.
 */
@Beta
public interface TransactionBuffer {

    /**
     * Return the metadata of a transaction in the buffer.
     *
     * @param txnID the transaction id
     * @return a future represents the result of the operation
     * @throws org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotFoundException if the transaction
     *         is not in the buffer.
     */
    CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID);

    /**
     * Append the buffer to the transaction buffer.
     *
     * <p>The entry will be indexed by <tt>txnId</tt> and <tt>sequenceId</tt>.
     *
     * @param txnId the transaction id
     * @param sequenceId the sequence id of the entry in this transaction buffer.
     * @param batchSize
     * @param buffer the entry buffer
     * @return a future represents the result of the operation.
     * @throws org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionSealedException if the transaction
     *         has been sealed.
     */
    CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, long batchSize, ByteBuf buffer);

    /**
     * Open a {@link TransactionBufferReader} to read entries of a given transaction
     * starting from the provided <tt>sequenceId</tt>.
     *
     * @param txnID transaction id
     * @param startSequenceId the sequence id to start read
     * @return a future represents the result of open operation.
     * @throws org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotFoundException if the transaction
     *         is not in the buffer.
     */
    CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId);

    /**
     * Handle TC endTxnOnPartition command
     *
     * @return
     */
    CompletableFuture<Void> endTxnOnPartition(TxnID txnID, int txnAction);

    /**
     * Append committed marker to the related origin topic partition.
     *
     * @param txnID transaction id
     * @return a future represents the position of the committed marker in the origin topic partition.
     */
    CompletableFuture<Position> commitPartitionTopic(TxnID txnID);

    /**
     * Commit the transaction and seal the buffer for this transaction.
     *
     * <p>If a transaction is sealed, no more entries can be {@link #appendBufferToTxn(TxnID, long, ByteBuf)}.
     *
     * @param txnID the transaction id
     * @param committedAtLedgerId the data ledger id where the commit marker of the transaction was appended to.
     * @param committedAtEntryId the data ledger id where the commit marker of the transaction was appended to.
     * @return a future represents the result of commit operation.
     * @throws org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotFoundException if the transaction
     *         is not in the buffer.
     */
    CompletableFuture<Void> commitTxn(TxnID txnID, long committedAtLedgerId, long committedAtEntryId);

    /**
     * Abort the transaction and all the entries of this transaction will
     * be discarded.
     *
     * @param txnID the transaction id
     * @return a future represents the result of abort operation.
     * @throws org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotFoundException if the transaction
     *         is not in the buffer.
     */
    CompletableFuture<Void> abortTxn(TxnID txnID);

    /**
     * Purge all the data of the transactions who are committed and stored
     * in the provided data ledgers.
     *
     * <p>This method will be called by the broker before they delete the ledgers.
     * It ensures that all the transactions committed in those ledgers can be purged.
     *
     * @param dataLedgers the list of data ledgers.
     * @return a future represents the result of purge operations.
     */
    CompletableFuture<Void> purgeTxns(List<Long> dataLedgers);

    /**
     * Close the buffer asynchronously.
     *
     * @return
     */
    CompletableFuture<Void> closeAsync();
}
