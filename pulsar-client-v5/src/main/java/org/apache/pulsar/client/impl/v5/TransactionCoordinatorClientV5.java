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

package org.apache.pulsar.client.impl.v5;

import org.apache.pulsar.client.api.v5.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.v5.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.v5.TxnID;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class TransactionCoordinatorClientV5 implements TransactionCoordinatorClient {

    final org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient v4TCC;

    TransactionCoordinatorClientV5(org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient v4TCC) {
        this.v4TCC = v4TCC;
    }

    @Override
    public void start() throws TransactionCoordinatorClientException {
        try {
            v4TCC.start();
        } catch (org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException e) {
            throw new TransactionCoordinatorClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        return v4TCC.startAsync();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return v4TCC.closeAsync();
    }

    @Override
    public TxnID newTransaction() throws TransactionCoordinatorClientException {
        try {
            return toV5(v4TCC.newTransaction());
        } catch (org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException e) {
            throw new TransactionCoordinatorClientException(e);
        }
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync() {
        return v4TCC.newTransactionAsync().thenApply(TransactionCoordinatorClientV5::toV5);
    }

    @Override
    public TxnID newTransaction(long timeout, TimeUnit unit) throws TransactionCoordinatorClientException {
        try {
            return toV5(v4TCC.newTransaction(timeout, unit));
        } catch (org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync(long timeout, TimeUnit unit) {
        return v4TCC.newTransactionAsync(timeout, unit).thenApply(TransactionCoordinatorClientV5::toV5);
    }

    @Override
    public void addPublishPartitionToTxn(TxnID txnID, List<String> partitions) throws TransactionCoordinatorClientException {
        try {
            v4TCC.addPublishPartitionToTxn(toV4(txnID), partitions);
        } catch (org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException e) {
            throw new TransactionCoordinatorClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> addPublishPartitionToTxnAsync(TxnID txnID, List<String> partitions) {
        return v4TCC.addPublishPartitionToTxnAsync(toV4(txnID), partitions);
    }

    @Override
    public void addSubscriptionToTxn(TxnID txnID, String topic, String subscription)
        throws TransactionCoordinatorClientException {
        try {
            v4TCC.addSubscriptionToTxn(toV4(txnID), topic, subscription);
        } catch (org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException e) {
            throw new TransactionCoordinatorClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> addSubscriptionToTxnAsync(TxnID txnID, String topic, String subscription) {
        return v4TCC.addSubscriptionToTxnAsync(toV4(txnID), topic, subscription);
    }

    @Override
    public void commit(TxnID txnID) throws TransactionCoordinatorClientException {
        try {
            v4TCC.commit(toV4(txnID));
        } catch (org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException e) {
            throw new TransactionCoordinatorClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> commitAsync(TxnID txnID) {
        return null;
    }

    @Override
    public void abort(TxnID txnID) throws TransactionCoordinatorClientException {
        try {
            v4TCC.abort(toV4(txnID));
        } catch (org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException e) {
            throw new TransactionCoordinatorClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> abortAsync(TxnID txnID) {
        return v4TCC.abortAsync(toV4(txnID));
    }

    @Override
    public State getState() {
        return switch (v4TCC.getState()) {
            case NONE -> State.NONE;
            case STARTING -> State.STARTING;
            case READY -> State.READY;
            case CLOSING -> State.CLOSING;
            case CLOSED ->  State.CLOSED;
        };
    }

    @Override
    public void close() throws IOException {
        v4TCC.close();
    }

    private static TxnID toV5(org.apache.pulsar.client.api.transaction.TxnID v4) {
        return new TxnID(v4.getMostSigBits(), v4.getLeastSigBits());
    }

    private static org.apache.pulsar.client.api.transaction.TxnID toV4(TxnID v4) {
        return new org.apache.pulsar.client.api.transaction.TxnID(v4.getMostSigBits(), v4.getLeastSigBits());
    }
}
