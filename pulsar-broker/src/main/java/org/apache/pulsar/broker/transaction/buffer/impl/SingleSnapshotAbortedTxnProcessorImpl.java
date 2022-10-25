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
package org.apache.pulsar.broker.transaction.buffer.impl;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.metadata.AbortTxnMetadata;
import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TxnIDData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class SingleSnapshotAbortedTxnProcessorImpl implements AbortedTxnProcessor {
    private final PersistentTopic topic;
    private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshot>> takeSnapshotWriter;
    private volatile PositionImpl maxReadPosition;

    private final Timer timer;

    /**
     * Aborts, map for jude message is aborted, linked for remove abort txn in memory when this
     * position have been deleted.
     */
    private final LinkedMap<TxnIDData, PositionImpl> aborts = new LinkedMap<>();

    private volatile long lastSnapshotTimestamps;
    private final int takeSnapshotIntervalNumber;

    private final int takeSnapshotIntervalTime;


    // when add abort or change max read position, the count will +1. Take snapshot will set 0 into it.
    private final AtomicLong changeMaxReadPositionAndAddAbortTimes = new AtomicLong();


    public SingleSnapshotAbortedTxnProcessorImpl(PersistentTopic topic) {
        this.topic = topic;
        this.maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
        this.takeSnapshotWriter = this.topic.getBrokerService().getPulsar()
                .getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotService().createWriter(TopicName.get(topic.getName()));
        this.timer = topic.getBrokerService().getPulsar().getTransactionTimer();
        this.takeSnapshotIntervalNumber = topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotMaxTransactionCount();
        this.takeSnapshotIntervalTime = topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotMinTimeInMillis();
        this.maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
    }

    @Override
    public void appendAbortedTxn(TxnIDData abortedTxnId, PositionImpl position) {
        aborts.put(abortedTxnId, position);
    }

    @Override
    public void updateMaxReadPosition(Position maxReadPosition) {
        if (this.maxReadPosition != maxReadPosition) {
            this.maxReadPosition = (PositionImpl) maxReadPosition;
            takeSnapshotByChangeTimes();
        }
    }

    @Override
    public void trimExpiredTxnIDDataOrSnapshotSegments() {
        while (!aborts.isEmpty() && !((ManagedLedgerImpl) topic.getManagedLedger())
                .ledgerExists(aborts.get(aborts.firstKey()).getLedgerId())) {
            if (log.isDebugEnabled()) {
                aborts.firstKey();
                log.debug("[{}] Topic transaction buffer clear aborted transaction, TxnId : {}, Position : {}",
                        topic.getName(), aborts.firstKey(), aborts.get(aborts.firstKey()));
            }
            aborts.remove(aborts.firstKey());
        }
    }

    @Override
    public boolean checkAbortedTransaction(TxnIDData txnID, Position readPosition) {
        return aborts.containsKey(txnID);
    }


    @Override
    public CompletableFuture<PositionImpl> recoverFromSnapshot(TopicTransactionBufferRecoverCallBack callBack) {
        return topic.getBrokerService().getPulsar().getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotService()
                .createReader(TopicName.get(topic.getName())).thenComposeAsync(reader -> {
                    PositionImpl startReadCursorPosition = null;
                    try {
                        boolean hasSnapshot = false;
                        while (reader.hasMoreEvents()) {
                            Message<TransactionBufferSnapshot> message = reader.readNext();
                            if (topic.getName().equals(message.getKey())) {
                                TransactionBufferSnapshot transactionBufferSnapshot = message.getValue();
                                if (transactionBufferSnapshot != null) {
                                    hasSnapshot = true;
                                    handleSnapshot(transactionBufferSnapshot);
                                    startReadCursorPosition = PositionImpl.get(
                                            transactionBufferSnapshot.getMaxReadPositionLedgerId(),
                                            transactionBufferSnapshot.getMaxReadPositionEntryId());
                                }
                            }
                        }
                        closeReader(reader);
                        if (!hasSnapshot) {
                            callBack.noNeedToRecover();
                            return null;
                        }
                        return CompletableFuture.completedFuture(startReadCursorPosition);
                    } catch (Exception ex) {
                        log.error("[{}] Transaction buffer recover fail when read "
                                + "transactionBufferSnapshot!", topic.getName(), ex);
                        callBack.recoverExceptionally(ex);
                        closeReader(reader);
                        return null;
                    }

                },  topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                        .getExecutor(this));
    }

    @Override
    public CompletableFuture<Void> clearSnapshot() {
        return this.takeSnapshotWriter.thenCompose(writer -> {
            TransactionBufferSnapshot snapshot = new TransactionBufferSnapshot();
            snapshot.setTopicName(topic.getName());
            return writer.deleteAsync(snapshot.getTopicName(), snapshot);
        }).thenCompose(__ -> CompletableFuture.completedFuture(null));
    }

    @Override
    public CompletableFuture<Void> takesFirstSnapshot() {
        return takeSnapshot();
    }

    @Override
    public PositionImpl getMaxReadPosition() {
        return maxReadPosition;
    }

    @Override
    public long getLastSnapshotTimestamps() {
        return this.lastSnapshotTimestamps;
    }

    private void closeReader(SystemTopicClient.Reader<TransactionBufferSnapshot> reader) {
        reader.closeAsync().exceptionally(e -> {
            log.error("[{}]Transaction buffer reader close error!", topic.getName(), e);
            return null;
        });
    }

    private void takeSnapshotByChangeTimes() {
        if (changeMaxReadPositionAndAddAbortTimes.incrementAndGet() >= takeSnapshotIntervalNumber) {
            takeSnapshot();
        }
    }

    private void takeSnapshotByTimeout() {
        if (changeMaxReadPositionAndAddAbortTimes.get() > 0) {
            takeSnapshot();
        }
        this.timer.newTimeout(SingleSnapshotAbortedTxnProcessorImpl.this,
                takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
    }

    private void handleSnapshot(TransactionBufferSnapshot snapshot) {
        maxReadPosition = PositionImpl.get(snapshot.getMaxReadPositionLedgerId(),
                snapshot.getMaxReadPositionEntryId());
        if (snapshot.getAborts() != null) {
            snapshot.getAborts().forEach(abortTxnMetadata ->
                    aborts.put(new TxnIDData(abortTxnMetadata.getTxnIdMostBits(),
                                    abortTxnMetadata.getTxnIdLeastBits()),
                            PositionImpl.get(abortTxnMetadata.getLedgerId(),
                                    abortTxnMetadata.getEntryId())));
        }
    }

    private CompletableFuture<Void> takeSnapshot() {
        changeMaxReadPositionAndAddAbortTimes.set(0);
        return takeSnapshotWriter.thenCompose(writer -> {
            TransactionBufferSnapshot snapshot = new TransactionBufferSnapshot();
            synchronized (SingleSnapshotAbortedTxnProcessorImpl.this) {
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
            }
            return writer.writeAsync(snapshot.getTopicName(), snapshot).thenAccept(messageId -> {
                this.lastSnapshotTimestamps = System.currentTimeMillis();
                if (log.isDebugEnabled()) {
                    log.debug("[{}]Transaction buffer take snapshot success! "
                            + "messageId : {}", topic.getName(), messageId);
                }
            }).exceptionally(e -> {
                log.warn("[{}]Transaction buffer take snapshot fail! ", topic.getName(), e);
                return null;
            });
        });
    }

    @Override
    public void run(Timeout timeout) {
        takeSnapshotByTimeout();
    }

}
