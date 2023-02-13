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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.metadata.AbortTxnMetadata;
import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class SingleSnapshotAbortedTxnProcessorImpl implements AbortedTxnProcessor {
    private final PersistentTopic topic;
    private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshot>> takeSnapshotWriter;
    /**
     * Aborts, map for jude message is aborted, linked for remove abort txn in memory when this
     * position have been deleted.
     */
    private final LinkedMap<TxnID, PositionImpl> aborts = new LinkedMap<>();

    private volatile long lastSnapshotTimestamps;

    public SingleSnapshotAbortedTxnProcessorImpl(PersistentTopic topic) {
        this.topic = topic;
        this.takeSnapshotWriter = this.topic.getBrokerService().getPulsar()
                .getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotService().createWriter(TopicName.get(topic.getName()));
        this.takeSnapshotWriter.exceptionally((ex) -> {
                    log.error("{} Failed to create snapshot writer", topic.getName());
                    topic.close();
                    return null;
                });
    }

    @Override
    public void putAbortedTxnAndPosition(TxnID abortedTxnId, PositionImpl abortedMarkerPersistentPosition) {
        aborts.put(abortedTxnId, abortedMarkerPersistentPosition);
    }

    //In this implementation we clear the invalid aborted txn ID one by one.
    @Override
    public void trimExpiredAbortedTxns() {
        while (!aborts.isEmpty() && !((ManagedLedgerImpl) topic.getManagedLedger())
                .ledgerExists(aborts.get(aborts.firstKey()).getLedgerId())) {
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

    private long getSystemClientOperationTimeoutMs() throws Exception {
        PulsarClientImpl pulsarClient = (PulsarClientImpl) topic.getBrokerService().getPulsar().getClient();
        return pulsarClient.getConfiguration().getOperationTimeoutMs();
    }

    @Override
    public CompletableFuture<PositionImpl> recoverFromSnapshot() {
        return topic.getBrokerService().getPulsar().getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotService()
                .createReader(TopicName.get(topic.getName())).thenComposeAsync(reader -> {
                    try {
                    PositionImpl startReadCursorPosition = null;
                        while (reader.hasMoreEvents()) {
                            Message<TransactionBufferSnapshot> message = reader.readNextAsync()
                                    .get(getSystemClientOperationTimeoutMs(), TimeUnit.MILLISECONDS);
                            if (topic.getName().equals(message.getKey())) {
                                TransactionBufferSnapshot transactionBufferSnapshot = message.getValue();
                                if (transactionBufferSnapshot != null) {
                                    handleSnapshot(transactionBufferSnapshot);
                                    startReadCursorPosition = PositionImpl.get(
                                            transactionBufferSnapshot.getMaxReadPositionLedgerId(),
                                            transactionBufferSnapshot.getMaxReadPositionEntryId());
                                }
                            }
                        }
                        return CompletableFuture.completedFuture(startReadCursorPosition);
                    } catch (TimeoutException ex) {
                        Throwable t = FutureUtil.unwrapCompletionException(ex);
                        String errorMessage = String.format("[%s] Transaction buffer recover fail by read "
                                + "transactionBufferSnapshot timeout!", topic.getName());
                        log.error(errorMessage, t);
                        return FutureUtil.failedFuture(
                                new BrokerServiceException.ServiceUnitNotReadyException(errorMessage, t));
                    } catch (Exception ex) {
                        log.error("[{}] Transaction buffer recover fail when read "
                                + "transactionBufferSnapshot!", topic.getName(), ex);
                        return FutureUtil.failedFuture(ex);
                    } finally {
                        closeReader(reader);
                    }
                },  topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                        .getExecutor(this));
    }

    @Override
    public CompletableFuture<Void> clearAbortedTxnSnapshot() {
        return this.takeSnapshotWriter.thenCompose(writer -> {
            TransactionBufferSnapshot snapshot = new TransactionBufferSnapshot();
            snapshot.setTopicName(topic.getName());
            return writer.deleteAsync(snapshot.getTopicName(), snapshot);
        }).thenRun(() -> log.info("[{}] Successes to delete the aborted transaction snapshot", this.topic));
    }

    @Override
    public CompletableFuture<Void> takeAbortedTxnsSnapshot(PositionImpl maxReadPosition) {
        return takeSnapshotWriter.thenCompose(writer -> {
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
    public long getLastSnapshotTimestamps() {
        return this.lastSnapshotTimestamps;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return takeSnapshotWriter.thenCompose(SystemTopicClient.Writer::closeAsync);
    }

    private void closeReader(SystemTopicClient.Reader<TransactionBufferSnapshot> reader) {
        reader.closeAsync().exceptionally(e -> {
            log.error("[{}]Transaction buffer reader close error!", topic.getName(), e);
            return null;
        });
    }

    private void handleSnapshot(TransactionBufferSnapshot snapshot) {
        if (snapshot.getAborts() != null) {
            snapshot.getAborts().forEach(abortTxnMetadata ->
                    aborts.put(new TxnID(abortTxnMetadata.getTxnIdMostBits(),
                                    abortTxnMetadata.getTxnIdLeastBits()),
                            PositionImpl.get(abortTxnMetadata.getLedgerId(),
                                    abortTxnMetadata.getEntryId())));
        }
    }

}
