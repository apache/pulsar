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

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.util.TransactionUtil;

/**
 * A class represents the metadata of a transaction stored in
 * the {@link org.apache.pulsar.transaction.coordinator.TransactionMetadataStore}.
 */
class TxnMetaImpl implements TxnMeta {

    private TxnID txnID;
    private Set<String> producedPartitions = new HashSet<>();
    private Set<TxnSubscription> ackedPartitions = new HashSet<>();
    private volatile TxnStatus txnStatus = TxnStatus.OPEN;
    private List<Position> positions = Collections.synchronizedList(new ArrayList<>());
    private Handle<TxnMetaImpl> recycleHandle;

    private static final Recycler<TxnMetaImpl> RECYCLER = new Recycler<TxnMetaImpl>() {
        protected TxnMetaImpl newObject(Recycler.Handle<TxnMetaImpl> handle) {
            return new TxnMetaImpl(handle);
        }
    };

    TxnMetaImpl(Handle<TxnMetaImpl> handle) {
        this.recycleHandle = handle;
    }

    // Constructor for transaction metadata
    static TxnMetaImpl create(TxnID txnID) {
        @SuppressWarnings("unchecked")
        TxnMetaImpl txnMeta = RECYCLER.get();
        txnMeta.txnID = txnID;
        return txnMeta;
    }

    public void recycle() {
        this.producedPartitions.clear();
        this.ackedPartitions.clear();
        this.txnStatus = TxnStatus.OPEN;
        this.positions.clear();

        if (recycleHandle != null) {
            recycleHandle.recycle(this);
        }
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
    public List<Position> positions() {
        return positions;
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

    @Override
    public TxnMeta addTxnPosition(Position position) {
        positions.add(position);
        return this;
    }
}
