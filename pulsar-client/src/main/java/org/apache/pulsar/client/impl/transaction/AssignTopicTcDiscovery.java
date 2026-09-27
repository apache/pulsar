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
package org.apache.pulsar.client.impl.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.CustomLog;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TransactionMetaStoreHandler;
import org.apache.pulsar.client.util.MathUtils;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;

/**
 * Coordinator discovery via the {@code transaction_coordinator_assign} partitioned topic — the
 * original mechanism. Each coordinator is a partition of the assign topic; the handler connects to
 * the broker that owns that partition's bundle (resolved by an ordinary topic lookup). Used against
 * brokers that don't advertise {@code supports_tc_metadata_discovery}.
 */
@CustomLog
class AssignTopicTcDiscovery implements TcDiscovery {

    private final PulsarClientImpl pulsarClient;
    private TransactionMetaStoreHandler[] handlers;
    private final ConcurrentLongHashMap<TransactionMetaStoreHandler> handlerMap =
            ConcurrentLongHashMap.<TransactionMetaStoreHandler>newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(1)
                    .build();
    private final AtomicLong epoch = new AtomicLong(0);

    AssignTopicTcDiscovery(PulsarClientImpl pulsarClient) {
        this.pulsarClient = pulsarClient;
    }

    @Override
    public CompletableFuture<Void> start() {
        return pulsarClient.getPartitionedTopicMetadata(
                        SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.getPartitionedTopicName(), true, false)
                .thenCompose(partitionMeta -> {
                    List<CompletableFuture<Void>> connectFutureList = new ArrayList<>();
                    log.debug().attr("partitions", partitionMeta.partitions)
                            .log("Transaction meta store assign partition is.");
                    if (partitionMeta.partitions > 0) {
                        handlers = new TransactionMetaStoreHandler[partitionMeta.partitions];
                        for (int i = 0; i < partitionMeta.partitions; i++) {
                            CompletableFuture<Void> connectFuture = new CompletableFuture<>();
                            connectFutureList.add(connectFuture);
                            TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                                    i, pulsarClient, getTCAssignTopicName(i), connectFuture);
                            handlers[i] = handler;
                            handlerMap.put(i, handler);
                            handler.start();
                        }
                    } else {
                        return FutureUtil.failedFuture(new TransactionCoordinatorClientException(
                                "The broker doesn't enable the transaction coordinator, "
                                        + "or the transaction coordinator has not initialized"));
                    }
                    return FutureUtil.waitForAll(connectFutureList);
                });
    }

    private static String getTCAssignTopicName(int partition) {
        return SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN
                + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
    }

    @Override
    public TransactionMetaStoreHandler handlerForCoordinator(long tcId) {
        return handlerMap.get(tcId);
    }

    @Override
    public TransactionMetaStoreHandler nextHandler() {
        if (handlers == null || handlers.length == 0) {
            return null;
        }
        int index = MathUtils.signSafeMod(epoch.incrementAndGet(), handlers.length);
        return handlers[index];
    }

    @Override
    public java.util.Collection<TransactionMetaStoreHandler> handlers() {
        TransactionMetaStoreHandler[] snapshot = handlers;
        return snapshot == null ? java.util.List.of() : java.util.List.of(snapshot);
    }

    @Override
    public void close() {
        if (handlers != null) {
            for (TransactionMetaStoreHandler handler : handlers) {
                try {
                    handler.close();
                } catch (IOException e) {
                    log.warn().exception(e).log("Close transaction meta store handler error");
                }
            }
            handlers = null;
        }
    }
}
