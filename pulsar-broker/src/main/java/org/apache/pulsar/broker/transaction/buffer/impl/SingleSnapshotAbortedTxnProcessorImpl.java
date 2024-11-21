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
package org.apache.pulsar.broker.transaction.buffer.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService.ReferenceCountedWriter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.metadata.AbortTxnMetadata;
import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;

@Slf4j
public class SingleSnapshotAbortedTxnProcessorImpl implements AbortedTxnProcessor {
    private final PersistentTopic topic;
    private final ReferenceCountedWriter<TransactionBufferSnapshot> takeSnapshotWriter;
    /**
     * Aborts, map for jude message is aborted, linked for remove abort txn in memory when this
     * position have been deleted.
     */
    private final LinkedMap<TxnID, Position> aborts = new LinkedMap<>();

    private volatile long lastSnapshotTimestamps;

    private volatile boolean isClosed = false;

    public SingleSnapshotAbortedTxnProcessorImpl(PersistentTopic topic) {
        this.topic = topic;
        this.takeSnapshotWriter = this.topic.getBrokerService().getPulsar()
                .getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotService().getReferenceWriter(TopicName.get(topic.getName()).getNamespaceObject());
        this.takeSnapshotWriter.getFuture().exceptionally((ex) -> {
                    log.error("{} Failed to create snapshot writer", topic.getName());
                    topic.close();
                    return null;
                });
    }

    @Override
    public void putAbortedTxnAndPosition(TxnID abortedTxnId, Position abortedMarkerPersistentPosition) {
        aborts.put(abortedTxnId, abortedMarkerPersistentPosition);
    }

    //In this implementation we clear the invalid aborted txn ID one by one.
    @Override
    public void trimExpiredAbortedTxns() {
        while (!aborts.isEmpty() && !topic.getManagedLedger().getLedgersInfo()
                .containsKey(aborts.get(aborts.firstKey()).getLedgerId())) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Topic transaction buffer clear aborted transaction, TxnId : {}, Position : {}",
                        topic.getName(), aborts.firstKey(), aborts.get(aborts.firstKey()));
            }
            aborts.remove(aborts.firstKey());
        }
    }

    @Override
    public boolean checkAbortedTransaction(TxnID txnID) {
        return aborts.containsKey(txnID);
    }

    @Override
    public CompletableFuture<Position> recoverFromSnapshot() {
        final var future = new CompletableFuture<Position>();
        final var pulsar = topic.getBrokerService().getPulsar();
        pulsar.getTransactionExecutorProvider().getExecutor(this).execute(() -> {
            try {
                final var snapshot = pulsar.getTransactionBufferSnapshotServiceFactory().getTxnBufferSnapshotService()
                        .getTableView().readLatest(topic.getName());
                if (snapshot != null) {
                    handleSnapshot(snapshot);
                    final var startReadCursorPosition = PositionFactory.create(snapshot.getMaxReadPositionLedgerId(),
                            snapshot.getMaxReadPositionEntryId());
                    future.complete(startReadCursorPosition);
                } else {
                    future.complete(null);
                }
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> clearAbortedTxnSnapshot() {
        return this.takeSnapshotWriter.getFuture().thenCompose(writer -> {
            TransactionBufferSnapshot snapshot = new TransactionBufferSnapshot();
            snapshot.setTopicName(topic.getName());
            return writer.deleteAsync(snapshot.getTopicName(), snapshot);
        }).thenRun(() -> log.info("[{}] Successes to delete the aborted transaction snapshot", this.topic));
    }

    @Override
    public CompletableFuture<Void> takeAbortedTxnsSnapshot(Position maxReadPosition) {
        return takeSnapshotWriter.getFuture().thenCompose(writer -> {
            TransactionBufferSnapshot snapshot = new TransactionBufferSnapshot();
            snapshot.setTopicName(topic.getName());
            snapshot.setMaxReadPositionLedgerId(maxReadPosition.getLedgerId());
            snapshot.setMaxReadPositionEntryId(maxReadPosition.getEntryId());
            List<AbortTxnMetadata> list = new ArrayList<>();
            aborts.forEach((k, v) -> {
                AbortTxnMetadata abortTxnMetadata = new AbortTxnMetadata();
                abortTxnMetadata.setTxnIdMostBits(k.getMostSigBits());
                abortTxnMetadata.setTxnIdLeastBits(k.getLeastSigBits());
                abortTxnMetadata.setLedgerId(v.getLedgerId());
                abortTxnMetadata.setEntryId(v.getEntryId());
                list.add(abortTxnMetadata);
            });
            snapshot.setAborts(list);
            return writer.writeAsync(snapshot.getTopicName(), snapshot).thenAccept(messageId -> {
                this.lastSnapshotTimestamps = System.currentTimeMillis();
                if (log.isDebugEnabled()) {
                    log.debug("[{}]Transaction buffer take snapshot success! "
                            + "messageId : {}", topic.getName(), messageId);
                }
            }).exceptionally(e -> {
                log.warn("[{}]Transaction buffer take snapshot fail! ", topic.getName(), e.getCause());
                return null;
            });
        });
    }

    @Override
    public TransactionBufferStats generateSnapshotStats(boolean segmentStats) {
        TransactionBufferStats transactionBufferStats = new TransactionBufferStats();
        transactionBufferStats.lastSnapshotTimestamps = this.lastSnapshotTimestamps;
        transactionBufferStats.totalAbortedTransactions = aborts.size();
        return transactionBufferStats;
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        if (!isClosed) {
            isClosed = true;
            takeSnapshotWriter.release();
        }
        return CompletableFuture.completedFuture(null);
    }

    private void handleSnapshot(TransactionBufferSnapshot snapshot) {
        if (snapshot.getAborts() != null) {
            snapshot.getAborts().forEach(abortTxnMetadata ->
                    aborts.put(new TxnID(abortTxnMetadata.getTxnIdMostBits(),
                                    abortTxnMetadata.getTxnIdLeastBits()),
                            PositionFactory.create(abortTxnMetadata.getLedgerId(),
                                    abortTxnMetadata.getEntryId())));
        }
    }

}
