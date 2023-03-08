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
package org.apache.pulsar.broker.transaction.recover;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;


/**
 * The transaction recover tracker implementation {@link TransactionRecoverTracker}.
 */
@Slf4j
public class TransactionRecoverTrackerImpl implements TransactionRecoverTracker {

    private final long tcId;
    private final TransactionMetadataStoreService transactionMetadataStoreService;
    private final TransactionTimeoutTracker timeoutTracker;

    /**
     * This is for recover open status transaction. The key is this transaction's sequenceId, the value is this
     * transaction timeout time.
     * <p>
     *     When transaction update status to committing or aborting, it will be remove form this.
     * <p>
     *     When transactionMetadataStore recover complete, the transaction don't update status, it will send all
     *     transaction to transactionTimeoutTracker.
     *
     */
    private final Map<Long, Long> openTransactions;

    /**
     * Update transaction to committing status.
     * <p>
     *     When transaction update status to committing, it will be add in.
     * <p>
     *     When transaction update status to committed status, the transaction will remove from it.
     * <p>
     *     When transactionMetadataStore recover complete, all transaction in this will endTransaction by commit action.
     */
    private final Map<Long, Long> committingTransactions;

    /**
     * Update transaction to aborting status.
     * <p>
     *     When transaction update status to aborting, it will be add in.
     * <p>
     *     When transaction update status to aborted status, the transaction will remove from it.
     * <p>
     *     When transactionMetadataStore recover complete, all transaction in this will endTransaction by abort action.
     */
    private final Map<Long, Long> abortingTransactions;

    /**
     * hold all committed or aborted transactions,those which is judged as
     * timeout will be removed both from terminatedTransactions and txnMetaMap.
     */
    private final LinkedMap<Long, Long> terminatedTransactions;

    public TransactionRecoverTrackerImpl(TransactionMetadataStoreService transactionMetadataStoreService,
                                         TransactionTimeoutTracker timeoutTracker, long tcId) {
        this.tcId = tcId;
        this.transactionMetadataStoreService = transactionMetadataStoreService;
        this.openTransactions = new HashMap<>();
        this.committingTransactions = new HashMap<>();
        this.abortingTransactions = new HashMap<>();
        this.terminatedTransactions = new LinkedMap<>();
        this.timeoutTracker = timeoutTracker;
    }

    @Override
    public void updateTransactionStatus(long sequenceId, TxnStatus txnStatus) throws InvalidTxnStatusException {
        switch (txnStatus) {
            case COMMITTING:
                committingTransactions.put(sequenceId, openTransactions.remove(sequenceId));
                break;
            case ABORTING:
                abortingTransactions.put(sequenceId, openTransactions.remove(sequenceId));
                break;
            case ABORTED:
                terminatedTransactions.put(sequenceId, abortingTransactions.remove(sequenceId));
                break;
            case COMMITTED:
                terminatedTransactions.put(sequenceId, committingTransactions.remove(sequenceId));
                break;
            default:
                throw new InvalidTxnStatusException("Transaction recover tracker`"
                        + new TxnID(tcId, sequenceId) + "` load replay metadata operation "
                        + "from transaction log with unknown operation");
        }
    }

    /**
     * @param sequenceId {@link Long} the sequenceId of this transaction.
     * @param timeout    {@link long} the timeout point(not duration) of this transaction.
     */
    @Override
    public void handleOpenStatusTransaction(long sequenceId, long timeout) {
        openTransactions.put(sequenceId, timeout);
    }

    @Override
    public void appendTransactionToTimeoutTracker(long unavailableDuration) {
        // add unavailable duration to timeout.
        for (long sequenceId : openTransactions.keySet()) {
            openTransactions.computeIfPresent(sequenceId, (k, v) -> v + unavailableDuration);
        }
        for (long sequenceId : committingTransactions.keySet()) {
            committingTransactions.computeIfPresent(sequenceId, (k, v) -> v + unavailableDuration);
        }
        for (long sequenceId : abortingTransactions.keySet()) {
            abortingTransactions.computeIfPresent(sequenceId, (k, v) -> v + unavailableDuration);
        }
        for (long sequenceId : terminatedTransactions.keySet()) {
            terminatedTransactions.computeIfPresent(sequenceId, (k, v) -> v + unavailableDuration);
        }
        openTransactions.forEach(timeoutTracker::replayAddTransaction);
        committingTransactions.forEach(timeoutTracker::replayAddTransaction);
        abortingTransactions.forEach(timeoutTracker::replayAddTransaction);
        terminatedTransactions.forEach(timeoutTracker::replayAddTransaction);
        committingTransactions.clear();
        abortingTransactions.clear();
        terminatedTransactions.clear();
    }

    @Override
    public void handleCommittedAbortedTransaction(
            long sequenceId, TxnStatus txnStatus, long unavailableDuration, Map terminatedTxnMetaMap) {
        // transaction in terminatedTransactions is ordered by added time.
        synchronized (this) {
            while (!terminatedTransactions.isEmpty() && terminatedTransactions.get(terminatedTransactions.firstKey())
                    + unavailableDuration > System.currentTimeMillis()) {
                long firstKey = terminatedTransactions.firstKey();
                terminatedTxnMetaMap.remove(firstKey);
                terminatedTransactions.remove(firstKey);
            }
        }
    }

    @Override
    public void handleCommittingAndAbortingTransaction() {
            committingTransactions.forEach((k, v) ->
                    transactionMetadataStoreService.endTransaction(new TxnID(tcId, k), TxnAction.COMMIT_VALUE,
                            false));

            abortingTransactions.forEach((k, v) ->
                    transactionMetadataStoreService.endTransaction(new TxnID(tcId, k), TxnAction.ABORT_VALUE,
                            false));
    }
}
