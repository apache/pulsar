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

import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.NonNull;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopicMetrics;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferClientStats;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;

public final class TransactionBufferClientStatsImpl implements TransactionBufferClientStats {
    private static final double[] QUANTILES = {0.50, 0.75, 0.95, 0.99, 0.999, 0.9999, 1};
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final PrometheusStats prometheusStats;

    public static final String PENDING_TRANSACTION_COUNTER = "pulsar.broker.transaction.buffer.client.pending.count";
    private final ObservableLongUpDownCounter pendingTransactionCounter;

    private final BrokerService brokerService;

    public TransactionBufferClientStatsImpl(@NonNull PulsarService pulsarService,
                                            boolean exposeTopicLevelMetrics,
                                            @NonNull TransactionBufferHandler handler) {
        this.brokerService = Objects.requireNonNull(pulsarService.getBrokerService());
        this.prometheusStats = PrometheusStats.getInstance(exposeTopicLevelMetrics, handler);
        this.pendingTransactionCounter = pulsarService.getOpenTelemetry().getMeter()
                .upDownCounterBuilder(PENDING_TRANSACTION_COUNTER)
                .setDescription("The number of pending transactions in the transaction buffer client.")
                .setUnit("{transaction}")
                .buildWithCallback(measurement -> measurement.record(handler.getPendingRequestsCount()));
    }

    @Override
    public void recordAbortFailed(String topic) {
        prometheusStats.abortFailed.labels(prometheusStats.labelValues(topic)).inc();
        getTransactionBufferClientMetrics(topic)
                .map(PersistentTopicMetrics.TransactionBufferClientMetrics::getAbortFailedCount)
                .ifPresent(LongAdder::increment);
    }

    @Override
    public void recordCommitFailed(String topic) {
        prometheusStats.commitFailed.labels(prometheusStats.labelValues(topic)).inc();
        getTransactionBufferClientMetrics(topic)
                .map(PersistentTopicMetrics.TransactionBufferClientMetrics::getCommitFailedCount)
                .ifPresent(LongAdder::increment);
    }

    @Override
    public void recordAbortLatency(String topic, long nanos) {
        prometheusStats.abortLatency.labels(prometheusStats.labelValues(topic)).observe(nanos);
        getTransactionBufferClientMetrics(topic)
                .map(PersistentTopicMetrics.TransactionBufferClientMetrics::getAbortSucceededCount)
                .ifPresent(LongAdder::increment);
    }

    @Override
    public void recordCommitLatency(String topic, long nanos) {
        prometheusStats.commitLatency.labels(prometheusStats.labelValues(topic)).observe(nanos);
        getTransactionBufferClientMetrics(topic)
                .map(PersistentTopicMetrics.TransactionBufferClientMetrics::getCommitSucceededCount)
                .ifPresent(LongAdder::increment);
    }

    private Optional<PersistentTopicMetrics.TransactionBufferClientMetrics> getTransactionBufferClientMetrics(
            String topic) {
        return brokerService.getTopicReference(topic)
                .filter(t -> t instanceof PersistentTopic)
                .map(t -> ((PersistentTopic) t).getPersistentTopicMetrics().getTransactionBufferClientMetrics());
    }

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            prometheusStats.close();
            pendingTransactionCounter.close();
        }
    }

    /**
     * The Prometheus collectors. They live in the JVM-wide default {@link CollectorRegistry}, which takes each metric
     * name once, so the clients of all the brokers in a JVM share them: the first client registers them, the first
     * client to close unregisters them, and a client created after that registers new ones.
     */
    private static final class PrometheusStats {
        private final AtomicBoolean closed = new AtomicBoolean(false);

        @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.TRANSACTION_BUFFER_CLIENT_OPERATION_COUNTER)
        private final Counter abortFailed;
        @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.TRANSACTION_BUFFER_CLIENT_OPERATION_COUNTER)
        private final Counter commitFailed;
        @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.TRANSACTION_BUFFER_CLIENT_OPERATION_COUNTER)
        private final Summary abortLatency;
        @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.TRANSACTION_BUFFER_CLIENT_OPERATION_COUNTER)
        private final Summary commitLatency;

        @PulsarDeprecatedMetric(newMetricName = PENDING_TRANSACTION_COUNTER)
        private final Gauge pendingRequests;

        private final boolean exposeTopicLevelMetrics;

        private static PrometheusStats instance;

        private PrometheusStats(boolean exposeTopicLevelMetrics, TransactionBufferHandler handler) {
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
                            return handler.getPendingRequestsCount();
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

        static synchronized PrometheusStats getInstance(boolean exposeTopicLevelMetrics,
                                                        TransactionBufferHandler handler) {
            if (null == instance) {
                instance = new PrometheusStats(exposeTopicLevelMetrics, handler);
            }
            return instance;
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

        void close() {
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
}
