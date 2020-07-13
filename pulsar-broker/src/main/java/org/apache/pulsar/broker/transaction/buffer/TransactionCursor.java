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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * The transaction Cursor maintains the index of all transactions.
 */
public interface TransactionCursor {
    /**
     * Get the specified transaction meta.
     *
     * @param txnID
     * @param createIfNotExist
     * @return
     */
    CompletableFuture<TransactionMeta> getTxnMeta(TxnID txnID, boolean createIfNotExist);

    /**
     * Commit transaction.
     *
     * @param committedLedgerId the ledger which  txn committed at.
     * @param committedEntryId  the entry which txn committed at.
     * @param txnID             the id which txn committed.
     * @param position          the commit position at transaction log.
     * @return
     */
    CompletableFuture<Void> commitTxn(long committedLedgerId, long committedEntryId, TxnID txnID, Position position);

    /**
     * Abort transaction.
     *
     * @param txnID aborted transaction id.
     * @return
     */
    CompletableFuture<Void> abortTxn(TxnID txnID);

    /**
     * Get all the transaction id on the specified ledger.
     *
     * @param ledgerId the transaction committed ledger id
     * @return
     */
    CompletableFuture<Set<TxnID>> getAllTxnsCommittedAtLedger(long ledgerId);

    /**
     * Remove the transactions on the specified ledger.
     *
     * @param ledgerId the remove transaction id
     * @return
     */
    CompletableFuture<Void> removeTxnsCommittedAtLedger(long ledgerId);
}
