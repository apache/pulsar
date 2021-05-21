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
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionStatusException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;

/**
 * The metadata for the transaction in the transaction buffer.
 */
@Beta
public interface TransactionMeta {

    /**
     * Returns the transaction id.
     *
     * @return the transaction id
     */
    TxnID id();

    /**
     * Return the status of the transaction.
     *
     * @return the status of the transaction
     */
    TxnStatus status();

    /**
     * Return the number of entries appended to the transaction.
     *
     * @return the number of entries
     */
    int numEntries();

    /**
     * Return messages number in one transaction.
     *
     * @return the number of transaction messages
     * @throws TransactionStatusException
     */
    int numMessageInTxn() throws TransactionStatusException;

    /**
     * Return the committed ledger id at data ledger.
     *
     * @return the committed ledger id
     */
    long committedAtLedgerId();

    /**
     * Return the committed entry id at data ledger.
     *
     * @return the committed entry id
     */
    long committedAtEntryId();

    /**
     * Return the last sequence id.
     *
     * @return the last sequence id
     */
    long lastSequenceId();

    /**
     * Read the entries from start sequence id.
     *
     * @param num the entries number need to read
     * @param startSequenceId the start position of the entries
     * @return
     */
    CompletableFuture<SortedMap<Long, Position>> readEntries(int num, long startSequenceId);

    /**
     * Add transaction entry into the transaction.
     *
     * @param sequenceId the message sequence id
     * @param position the position of transaction log
     * @param batchSize
     * @return
     */
    CompletableFuture<Position> appendEntry(long sequenceId, Position position, int batchSize);

    /**
     * Mark the transaction status is committing.
     * @return
     */
    CompletableFuture<TransactionMeta> committingTxn();

    /**
     * Mark the transaction is committed.
     *
     * @param committedAtLedgerId
     * @param committedAtEntryId
     * @return
     */
    CompletableFuture<TransactionMeta> commitTxn(long committedAtLedgerId, long committedAtEntryId);

    /**
     * Mark the transaction is aborted.
     *
     * @return
     */
    CompletableFuture<TransactionMeta> abortTxn();

}
