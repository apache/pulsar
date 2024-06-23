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
package org.apache.pulsar.transaction.coordinator.impl;

import static org.apache.commons.lang3.StringUtils.isBlank;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.prometheus.client.Summary;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.RecoverTimeRecord;
import org.apache.pulsar.transaction.coordinator.TerminatedTransactionMetadataEntry;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataPreserver;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A transaction metadata preserver implementation.
 * This class is used to persist the transaction metadata of terminated txn to a compacted topic.
 * As all methods are executed in single thread poll executor, we do not need to consider the
 * concurrent problem.
 */
public class MLTransactionMetadataPreserverImpl implements TransactionMetadataPreserver{
    private static final Logger log = LoggerFactory.getLogger(MLTransactionMetadataPreserverImpl.class);
    public static final String TRANSACTION_METADATA_PERSIST_TOPIC_PREFIX = "__terminated_txn_state_";

    private final int transactionMetaPersistCount;
    private final long transactionMetaPersistTimeInMS;
    private final long transactionMetaExpireCheckIntervalInMS;
    private final long tcID;
    private final Producer<TerminatedTransactionMetadataEntry> producer;
    private final Reader<TerminatedTransactionMetadataEntry> reader;
    // key is the client name, value is the list of transaction metadata of terminated txn.
    public final Map<String, LinkedList<TxnMeta>> terminatedTxnMetaList = new HashMap<>();
    // key is the client name, value is the map mapping transaction id of terminated txn to txnMeta.
    public final Map<String, Map<TxnID, TxnMeta>> terminatedTxnMetaMap = new HashMap<>();

    public final Set<String> needToFlush = new HashSet<>();

    public final RecoverTimeRecord recoverTime = new RecoverTimeRecord();

    private static final double[] QUANTILES = {0.50, 0.95, 0.99, 1};

    private static final Summary produceLatency = buildSummary("pulsar_txn_preserver_produce_latency", "-",
            new String[]{"coordinator_id"});


    /**
     * do not enable terminated transaction metadata persist.
     */
    public MLTransactionMetadataPreserverImpl() {
        this.transactionMetaPersistCount = 0;
        this.transactionMetaPersistTimeInMS = 0;
        this.transactionMetaExpireCheckIntervalInMS = 0;
        this.tcID = -1;
        this.producer = null;
        this.reader = null;
    }

    // enable terminated transaction metadata persist
    public MLTransactionMetadataPreserverImpl(TransactionCoordinatorID tcID,
                                              int transactionMetaPersistCount,
                                              long transactionMetaPersistTimeInHour,
                                              long transactionMetadataExpireIntervalInSecond,
                                              PulsarClient pulsarClient) {
        if (transactionMetaPersistCount <= 0 || pulsarClient == null
                || pulsarClient.isClosed() || tcID == null) {
            // do not enable terminated transaction metadata persist
            log.info("Transaction metadata preserver init failed, transaction metadata persist count is {}, "
                    + "pulsar client is null or closed, or transaction coordinator id is null.",
                    transactionMetaPersistCount);
            this.transactionMetaPersistCount = 0;
            this.transactionMetaPersistTimeInMS = 0;
            this.transactionMetaExpireCheckIntervalInMS = 0;
            this.tcID = -1;
            this.producer = null;
            this.reader = null;
            return;
        }

        this.tcID = tcID.getId();
        this.transactionMetaPersistCount = transactionMetaPersistCount;
        this.transactionMetaExpireCheckIntervalInMS = transactionMetadataExpireIntervalInSecond * 1000;
        this.transactionMetaPersistTimeInMS = transactionMetaPersistTimeInHour * 60 * 60 * 1000;
        String topicName = getTransactionMetadataPersistTopicName(tcID);
        recoverTime.setRecoverStartTime(System.currentTimeMillis());
        this.producer = pulsarClient.newProducer(Schema.JSON(TerminatedTransactionMetadataEntry.class))
                .topic(topicName)
                .createAsync().thenCompose(producer -> {
                    log.info("Create producer for transaction metadata persist topic {} successfully.", topicName);
                    return CompletableFuture.completedFuture(producer);
                }).join();
        this.reader = pulsarClient.newReader(Schema.JSON(TerminatedTransactionMetadataEntry.class))
                .topic(topicName)
                .startMessageId(MessageId.earliest)
                .readCompacted(true)
                .createAsync().thenCompose(reader -> {
                    log.info("Create reader for transaction metadata persist topic {} successfully.", topicName);
                    return CompletableFuture.completedFuture(reader);
                }).join();
        log.info("Transaction metadata preserver init successfully, transaction coordinator id is {}, "
                + "transaction metadata persist count is {}.", tcID.getId(), transactionMetaPersistCount);
    }

    private static String getTransactionMetadataPersistTopicName(TransactionCoordinatorID tcID) {
        return TopicName.get(TopicDomain.persistent.value(), NamespaceName.SYSTEM_NAMESPACE,
                TRANSACTION_METADATA_PERSIST_TOPIC_PREFIX + tcID.getId()).toString();
    }

    /**
     * Replay transaction metadata to initialize the terminatedTxnMetaMap.
     */
    @Override
    public void replay() throws PulsarClientException {
        if (!enabled()) {
            log.info("Transaction metadata preserver is not enabled, do not replay transaction metadata.");
            return;
        }
        try {
            while (reader.hasMessageAvailable()) {
                Message<TerminatedTransactionMetadataEntry> entry = reader.readNext();
                String clientName = entry.getKey();
                LinkedList<TxnMeta> txnMetaList = entry.getValue().getTxnMetas();
                terminatedTxnMetaList.put(clientName, txnMetaList);
                HashMap<TxnID, TxnMeta> txnMetaMap = new HashMap<>();
                terminatedTxnMetaMap.put(clientName, txnMetaMap);
                for (TxnMeta txnMeta : txnMetaList) {
                    txnMetaMap.put(txnMeta.id(), txnMeta);
                }
                if (log.isDebugEnabled()) {
                    log.debug("Replay transaction metadata, tcID:{}, client name:{}.", tcID, clientName);
                }
            }
            recoverTime.setRecoverEndTime(System.currentTimeMillis());
            log.info("Replay transaction metadata successfully, tcID:{}.", tcID);
        } catch (Exception e) {
            // Though replay transaction metadata failed, the transaction coordinator can still work.
            log.error("Replay transaction metadata failed, tcID:{}, reason:{}.", tcID, e);
            throw e;
        } finally {
            reader.closeAsync();
        }
    }


    /**
     * Close the transaction metadata preserver.
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        if (producer != null) {
            return producer.closeAsync();
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Append the transaction metadata to the system topic __terminated_txn_state.
     * append method will be called after the transaction become aborting, but
     * before the endTxn command is sent to TB/TP; and flush the messages before the
     * transaction is terminated, the duration between append and flush method
     * can improve the produce efficiency with deduplication of those messages with
     * same clientName.
     *
     * @param txnMeta   the transaction metadata
     * @return
     */
    @Override
    public void append(TxnMeta txnMeta, String clientName) {
        if (!enabled() || clientName == null) {
            return;
        }
        if (terminatedTxnMetaMap.containsKey(clientName)) {
            List<TxnMeta> txnMetaList = terminatedTxnMetaList.get(clientName);
            Map<TxnID, TxnMeta> txnIDTxnMetaMap = terminatedTxnMetaMap.get(clientName);
            // check duplicate
            if (txnIDTxnMetaMap.containsKey(txnMeta.id())) {
                return;
            } else {
                txnMetaList.add(txnMeta);
                txnIDTxnMetaMap.put(txnMeta.id(), txnMeta);
                while (txnMetaList.size() > transactionMetaPersistCount) {
                    txnIDTxnMetaMap.remove(txnMetaList.remove(0).id());
                }
            }
        } else {
            LinkedList<TxnMeta> txnMetaList = new LinkedList<>();
            Map<TxnID, TxnMeta> txnIDTxnMetaMap = new HashMap<>();
            txnMetaList.add(txnMeta);
            txnIDTxnMetaMap.put(txnMeta.id(), txnMeta);
            terminatedTxnMetaList.put(clientName, txnMetaList);
            terminatedTxnMetaMap.put(clientName, txnIDTxnMetaMap);
        }
        if (log.isDebugEnabled()) {
            log.debug("Append transaction metadata, client name:{}, transaction id:{}, tcID:{}.",
                    clientName, txnMeta.id(), tcID);
        }
        needToFlush.add(clientName);
    }

    /**
     * flush the transaction metadata to the system topic __terminated_txn_state
     * before the state of txn is terminated.
     */
    @Override
    public void flush(String clientName) throws CoordinatorException.PreserverClosedException {
        if (!needToFlush.contains(clientName)) {
            if (log.isDebugEnabled()) {
                log.debug("No need to flush transaction metadata for client name:{}, "
                        + "previous flush method has done it. tcID:{}.", clientName, tcID);
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Flush transaction metadata, client name:{}, tcID:{}.", clientName, tcID);
        }
        TerminatedTransactionMetadataEntry entry = new TerminatedTransactionMetadataEntry();
        entry.setTxnMetas(terminatedTxnMetaList.get(clientName));
        try {
            long start = System.currentTimeMillis();
            producer.newMessage().key(clientName).value(entry).send();
            produceLatency.labels(String.valueOf(tcID)).observe(System.currentTimeMillis() - start);
        } catch (PulsarClientException e) {
            log.error("Flush transaction metadata failed, client name:{}, tcID:{}, reason:{}.",
                    clientName, tcID, e);
            throw new CoordinatorException.PreserverClosedException(e.getMessage());
        }
        needToFlush.remove(clientName);
    }

    @Override
    public boolean enabled() {
        return transactionMetaPersistCount > 0;
    }

    @Override
    public TxnMeta getTxnMeta(TxnID txnID, String clientName) {
        if (!enabled() || isBlank(clientName)) {
            return null;
        }
        // this method is not executed in single thread, so we need to ensure
        // NPE exception will not be thrown.
        Map<TxnID, TxnMeta> txnIDTxnMetaMap = terminatedTxnMetaMap.get(clientName);
        if (txnIDTxnMetaMap == null) {
            return null;
        }
        return txnIDTxnMetaMap.get(txnID);
    }

    @Override
    public void expireTransactionMetadata() {
        if (!enabled()) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Start to check transaction metadata expire, tcID:{}.", tcID);
        }
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, LinkedList<TxnMeta>>> iterator =
                terminatedTxnMetaList.entrySet().iterator();
        Map.Entry<String, LinkedList<TxnMeta>> entry;
        while (iterator.hasNext()) {
            entry = iterator.next();
            String clientName = entry.getKey();
            LinkedList<TxnMeta> txnMetaList = entry.getValue();
            Map<TxnID, TxnMeta> txnIDTxnMetaMap = terminatedTxnMetaMap.get(clientName);
            while (!txnMetaList.isEmpty()) {
                // peek the oldest transaction metadata
                TxnMeta txnMeta = txnMetaList.peek();
                if (now - txnMeta.getTimeoutAt() - txnMeta.getOpenTimestamp()
                        > transactionMetaPersistTimeInMS) {
                    txnMetaList.remove();
                    txnIDTxnMetaMap.remove(txnMeta.id());
                } else {
                    break;
                }
            }
            if (txnMetaList.isEmpty()) {
                // delete the transaction metadata from the system topic __terminated_txn_state
                // producer.newMessage().key(clientName).value(null).send();
                iterator.remove();
                terminatedTxnMetaMap.remove(clientName);
            }
        }
    }

    @Override
    public long getExpireOldTransactionMetadataIntervalMS() {
        return transactionMetaExpireCheckIntervalInMS;
    }


    @Override
    public long getRecoveryTime() {
        return recoverTime.getRecoverEndTime() - recoverTime.getRecoverStartTime();
    }

    private static Summary buildSummary(String name, String help, String[] labelNames) {
        Summary.Builder builder = Summary.build(name, help)
                .labelNames(labelNames);
        for (double quantile : QUANTILES) {
            builder.quantile(quantile, 0.01D);
        }
        return builder.register();
    }
}