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
package org.apache.pulsar.transaction.coordinator.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.apache.pulsar.transaction.coordinator.util.TransactionUtil;

/**
 * A class represents the metadata of a transaction stored in
 * the {@link org.apache.pulsar.transaction.coordinator.TransactionMetadataStore}.
 */
class TxnMetaImpl implements TxnMeta {

    private final TxnID txnID;
    private final Set<String> producedPartitions = new HashSet<>();
    private final Set<TransactionSubscription> ackedPartitions = new HashSet<>();
    private volatile TxnStatus txnStatus = TxnStatus.OPEN;
    private final long openTimestamp;
    private final long timeoutAt;
    private final String owner;

    TxnMetaImpl(TxnID txnID, long openTimestamp, long timeoutAt, String owner) {
        this.txnID = txnID;
        this.openTimestamp = openTimestamp;
        this.timeoutAt = timeoutAt;
        this.owner = owner;
    }

    @Override
    public TxnID id() {
        return txnID;
    }

    /**
     * Return the current status of the transaction.
     *
     * @return current status of the transaction.
     */
    @Override
    public synchronized TxnStatus status() {
        return txnStatus;
    }

    @Override
    public List<String> producedPartitions() {
        List<String> returnedPartitions;
        synchronized (this) {
            returnedPartitions = new ArrayList<>(producedPartitions.size());
            returnedPartitions.addAll(producedPartitions);
        }
        Collections.sort(returnedPartitions);
        return returnedPartitions;
    }

    @Override
    public List<TransactionSubscription> ackedPartitions() {
        List<TransactionSubscription> returnedPartitions;
        synchronized (this) {
            returnedPartitions = new ArrayList<>(ackedPartitions.size());
            returnedPartitions.addAll(ackedPartitions);
        }
        Collections.sort(returnedPartitions);
        return returnedPartitions;
    }

    /**
     * Check if the transaction is in an expected status.
     *
     * @param expectedStatus
     */
    private synchronized void checkTxnStatus(TxnStatus expectedStatus) throws InvalidTxnStatusException {
        if (this.txnStatus != expectedStatus) {
            throw new InvalidTxnStatusException(
                txnID, expectedStatus, txnStatus
            );
        }
    }

    /**
     * Add the list partitions that the transaction produces to.
     *
     * @param partitions the list of partitions that the txn produces to
     * @return the transaction itself.
     * @throws InvalidTxnStatusException
     */
    @Override
    public synchronized TxnMetaImpl addProducedPartitions(List<String> partitions) throws InvalidTxnStatusException {
        checkTxnStatus(TxnStatus.OPEN);

        this.producedPartitions.addAll(partitions);
        return this;
    }

    /**
     * Remove the list partitions that the transaction acknowledges to.
     *
     * @param partitions the list of partitions that the txn acknowledges to
     * @return the transaction itself.
     * @throws InvalidTxnStatusException
     */
    @Override
    public synchronized TxnMetaImpl addAckedPartitions(List<TransactionSubscription> partitions)
            throws InvalidTxnStatusException {
        checkTxnStatus(TxnStatus.OPEN);
        this.ackedPartitions.addAll(partitions);
        return this;
    }

    /**
     * Update the transaction stats from the <tt>newStatus</tt> only when
     * the current status is the expected <tt>expectedStatus</tt>.
     *
     * @param newStatus the new transaction status
     * @param expectedStatus the expected transaction status
     * @return the transaction itself.
     * @throws InvalidTxnStatusException
     */
    @Override
    public synchronized TxnMetaImpl updateTxnStatus(TxnStatus newStatus,
                                                    TxnStatus expectedStatus)
        throws InvalidTxnStatusException {
        checkTxnStatus(expectedStatus);
        if (!TransactionUtil.canTransitionTo(txnStatus, newStatus)) {
            throw new InvalidTxnStatusException(
                "Transaction `" + txnID + "` CANNOT transaction from status " + txnStatus + " to " + newStatus);
        }
        this.txnStatus = newStatus;
        return this;
    }

    @Override
    public long getOpenTimestamp() {
        return this.openTimestamp;
    }

    @Override
    public long getTimeoutAt() {
        return this.timeoutAt;
    }

    @Override
    public String getOwner() {
        return this.owner;
    }
}
