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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferClientStats;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.common.naming.TopicName;

public final class TransactionBufferClientStatsImpl implements TransactionBufferClientStats {
    private static final double[] QUANTILES = {0.50, 0.75, 0.95, 0.99, 0.999, 0.9999, 1};
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Counter abortFailed;
    private final Counter commitFailed;
    private final Summary abortLatency;
    private final Summary commitLatency;
    private final Gauge pendingRequests;

    private final boolean exposeTopicLevelMetrics;

    private static TransactionBufferClientStats instance;

    private TransactionBufferClientStatsImpl(boolean exposeTopicLevelMetrics,
                                             TransactionBufferHandler handler) {
        this.exposeTopicLevelMetrics = exposeTopicLevelMetrics;
        String[] labelNames = exposeTopicLevelMetrics
                ? new String[]{"namespace", "topic"} : new String[]{"namespace"};

        this.abortFailed = Counter.build("pulsar_txn_tb_client_abort_failed", "-")
                .labelNames(labelNames)
                .register();
        this.commitFailed = Counter.build("pulsar_txn_tb_client_commit_failed", "-")
                .labelNames(labelNames)
                .register();
        this.abortLatency =
                this.buildSummary("pulsar_txn_tb_client_abort_latency", "-", labelNames);
        this.commitLatency =
                this.buildSummary("pulsar_txn_tb_client_commit_latency", "-", labelNames);

        this.pendingRequests = Gauge.build("pulsar_txn_tb_client_pending_requests", "-")
                .register()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return null == handler ? 0 : handler.getPendingRequestsCount();
                    }
                });
    }

    private Summary buildSummary(String name, String help, String[] labelNames) {
        Summary.Builder builder = Summary.build(name, help)
                .labelNames(labelNames);
        for (double quantile : QUANTILES) {
            builder.quantile(quantile, 0.01D);
        }
        return builder.register();
    }

    public static synchronized TransactionBufferClientStats getInstance(boolean exposeTopicLevelMetrics,
                                                                        TransactionBufferHandler handler) {
        if (null == instance) {
            instance = new TransactionBufferClientStatsImpl(exposeTopicLevelMetrics, handler);
        }

        return instance;
    }

    @Override
    public void recordAbortFailed(String topic) {
        this.abortFailed.labels(labelValues(topic)).inc();
    }

    @Override
    public void recordCommitFailed(String topic) {
        this.commitFailed.labels(labelValues(topic)).inc();
    }

    @Override
    public void recordAbortLatency(String topic, long nanos) {
        this.abortLatency.labels(labelValues(topic)).observe(nanos);
    }

    @Override
    public void recordCommitLatency(String topic, long nanos) {
        this.commitLatency.labels(labelValues(topic)).observe(nanos);
    }

    private String[] labelValues(String topic) {
        try {
            TopicName topicName = TopicName.get(topic);
            return exposeTopicLevelMetrics
                    ? new String[]{topicName.getNamespace(), topic} : new String[]{topicName.getNamespace()};
        } catch (Throwable t) {
            return exposeTopicLevelMetrics ? new String[]{"unknown", "unknown"} : new String[]{"unknown"};
        }
    }

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            instance = null;
            CollectorRegistry.defaultRegistry.unregister(this.abortFailed);
            CollectorRegistry.defaultRegistry.unregister(this.commitFailed);
            CollectorRegistry.defaultRegistry.unregister(this.abortLatency);
            CollectorRegistry.defaultRegistry.unregister(this.commitLatency);
            CollectorRegistry.defaultRegistry.unregister(this.pendingRequests);
        }
    }
}
