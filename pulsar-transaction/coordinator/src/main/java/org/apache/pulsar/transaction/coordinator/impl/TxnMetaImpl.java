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

import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.util.TransactionUtil;
import org.apache.pulsar.transaction.impl.common.TxnID;

/**
 * A class represents the metadata of a transaction stored in
 * the {@link org.apache.pulsar.transaction.coordinator.TransactionMetadataStore}.
 */
class TxnMetaImpl implements TxnMeta {

    private final TxnID txnID;
    private final Set<String> producedPartitions = new HashSet<>();
    private final Set<TxnSubscription> ackedPartitions = new HashSet<>();
    private volatile TxnStatus txnStatus;

    TxnMetaImpl(TxnID txnID) {
        this.txnID = txnID;
        this.txnStatus = TxnStatus.OPEN;
    }

    @Override
    public TxnID id() {
        return txnID;
    }

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

    public List<TxnSubscription> ackedPartitions() {
        List<TxnSubscription> returnedSubscriptions;
        synchronized (this) {
            returnedSubscriptions = new ArrayList<>(ackedPartitions.size());
            returnedSubscriptions.addAll(ackedPartitions);
        }
        return returnedSubscriptions;
    }

    @Override
    public synchronized void checkTxnStatus(TxnStatus expectedStatus) throws InvalidTxnStatusException {
        if (this.txnStatus != expectedStatus) {
            throw new InvalidTxnStatusException(
                txnID, expectedStatus, txnStatus
            );
        }
    }

    @Override
    public synchronized TxnMetaImpl addProducedPartitions(List<String> partitions) throws InvalidTxnStatusException {
        checkTxnStatus(TxnStatus.OPEN);

        this.producedPartitions.addAll(partitions);
        return this;
    }

    @Override
    public synchronized TxnMeta addAckedPartitions(List<TxnSubscription> partitions) throws InvalidTxnStatusException {
        checkTxnStatus(TxnStatus.OPEN);

        this.ackedPartitions.addAll(partitions);
        return this;
    }

    @Override
    public synchronized TxnMetaImpl updateTxnStatus(TxnStatus newStatus,
                                                    TxnStatus expectedStatus)
        throws InvalidTxnStatusException {
        checkTxnStatus(expectedStatus);
        if (!TransactionUtil.canTransitionTo(txnStatus, newStatus)) {
            throw new InvalidTxnStatusException(txnID, expectedStatus, newStatus);
        }
        this.txnStatus = newStatus;
        return this;
    }
}
