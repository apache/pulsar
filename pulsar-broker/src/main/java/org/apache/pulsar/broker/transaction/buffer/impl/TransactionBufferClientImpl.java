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

import io.netty.util.HashedWheelTimer;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferClientStats;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.common.api.proto.TxnAction;

/**
 * The implementation of {@link TransactionBufferClient}.
 */
@Slf4j
public class TransactionBufferClientImpl implements TransactionBufferClient {

    private final TransactionBufferHandler tbHandler;
    private final TransactionBufferClientStats stats;

    private TransactionBufferClientImpl(TransactionBufferHandler tbHandler, boolean exposeTopicLevelMetrics,
                                        boolean enableTxnCoordinator) {
        this.tbHandler = tbHandler;
        this.stats = TransactionBufferClientStats.create(exposeTopicLevelMetrics, tbHandler, enableTxnCoordinator);
    }

    public static TransactionBufferClient create(PulsarService pulsarService, HashedWheelTimer timer,
        int maxConcurrentRequests, long operationTimeoutInMills) throws PulsarServerException {
        TransactionBufferHandler handler = new TransactionBufferHandlerImpl(pulsarService, timer,
                maxConcurrentRequests, operationTimeoutInMills);

        ServiceConfiguration config = pulsarService.getConfig();
        boolean exposeTopicLevelMetrics = config.isExposeTopicLevelMetricsInPrometheus();
        boolean enableTxnCoordinator = config.isTransactionCoordinatorEnabled();
        return new TransactionBufferClientImpl(handler, exposeTopicLevelMetrics, enableTxnCoordinator);
    }

    @Override
    public CompletableFuture<TxnID> commitTxnOnTopic(String topic, long txnIdMostBits,
                                                     long txnIdLeastBits, long lowWaterMark) {
        long start = System.nanoTime();
        return tbHandler.endTxnOnTopic(topic, txnIdMostBits, txnIdLeastBits, TxnAction.COMMIT, lowWaterMark)
                .whenComplete((__, t) -> {
                    if (null != t) {
                        this.stats.recordCommitFailed(topic);
                    } else {
                        this.stats.recordCommitLatency(topic, System.nanoTime() - start);
                    }
                });
    }

    @Override
    public CompletableFuture<TxnID> abortTxnOnTopic(String topic, long txnIdMostBits,
                                                    long txnIdLeastBits, long lowWaterMark) {
        long start = System.nanoTime();
        return tbHandler.endTxnOnTopic(topic, txnIdMostBits, txnIdLeastBits, TxnAction.ABORT, lowWaterMark)
                .whenComplete((__, t) -> {
                    if (null != t) {
                        this.stats.recordAbortFailed(topic);
                    } else {
                        this.stats.recordAbortLatency(topic, System.nanoTime() - start);
                    }
                });
    }

    @Override
    public CompletableFuture<TxnID> commitTxnOnSubscription(String topic, String subscription, long txnIdMostBits,
                                                            long txnIdLeastBits, long lowWaterMark) {
        long start = System.nanoTime();
        return tbHandler.endTxnOnSubscription(topic, subscription, txnIdMostBits, txnIdLeastBits,
                TxnAction.COMMIT, lowWaterMark)
                .whenComplete((__, t) -> {
                    if (null != t) {
                        this.stats.recordCommitFailed(topic);
                    } else {
                        this.stats.recordCommitLatency(topic, System.nanoTime() - start);
                    }
                });
    }

    @Override
    public CompletableFuture<TxnID> abortTxnOnSubscription(String topic, String subscription,
                                                           long txnIdMostBits, long txnIdLeastBits, long lowWaterMark) {
        long start = System.nanoTime();
        return tbHandler.endTxnOnSubscription(topic, subscription, txnIdMostBits, txnIdLeastBits,
                TxnAction.ABORT, lowWaterMark)
                .whenComplete((__, t) -> {
                    if (null != t) {
                        this.stats.recordAbortFailed(topic);

                    } else {
                        this.stats.recordAbortLatency(topic, System.nanoTime() - start);
                    }
                });
    }

    @Override
    public void close() {
        tbHandler.close();
        this.stats.close();
    }

    @Override
    public int getAvailableRequestCredits() {
        return tbHandler.getAvailableRequestCredits();
    }

    @Override
    public int getPendingRequestsCount() {
        return tbHandler.getPendingRequestsCount();
    }
}
