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
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
    /** simpleclient's default sliding window for summary quantiles; the 1.x default is 5 minutes. */
    private static final long QUANTILE_WINDOW_SECONDS = TimeUnit.MINUTES.toSeconds(10);
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
        prometheusStats.abortFailed.labelValues(prometheusStats.labelValues(topic)).inc();
        getTransactionBufferClientMetrics(topic)
                .map(PersistentTopicMetrics.TransactionBufferClientMetrics::getAbortFailedCount)
                .ifPresent(LongAdder::increment);
    }

    @Override
    public void recordCommitFailed(String topic) {
        prometheusStats.commitFailed.labelValues(prometheusStats.labelValues(topic)).inc();
        getTransactionBufferClientMetrics(topic)
                .map(PersistentTopicMetrics.TransactionBufferClientMetrics::getCommitFailedCount)
                .ifPresent(LongAdder::increment);
    }

    @Override
    public void recordAbortLatency(String topic, long nanos) {
        prometheusStats.abortLatency.labelValues(prometheusStats.labelValues(topic)).observe(nanos);
        getTransactionBufferClientMetrics(topic)
                .map(PersistentTopicMetrics.TransactionBufferClientMetrics::getAbortSucceededCount)
                .ifPresent(LongAdder::increment);
    }

    @Override
    public void recordCommitLatency(String topic, long nanos) {
        prometheusStats.commitLatency.labelValues(prometheusStats.labelValues(topic)).observe(nanos);
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
     * The Prometheus collectors. They live in the JVM-wide default {@link PrometheusRegistry}, which takes each metric
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
        private final GaugeWithCallback pendingRequests;

        private final boolean exposeTopicLevelMetrics;

        private static PrometheusStats instance;

        private PrometheusStats(boolean exposeTopicLevelMetrics, TransactionBufferHandler handler) {
            this.exposeTopicLevelMetrics = exposeTopicLevelMetrics;
            String[] labelNames = exposeTopicLevelMetrics
                    ? new String[]{"namespace", "topic"} : new String[]{"namespace"};

            this.abortFailed = Counter.builder().name("pulsar_txn_tb_client_abort_failed").help("-")
                    .labelNames(labelNames)
                    .register();
            this.commitFailed = Counter.builder().name("pulsar_txn_tb_client_commit_failed").help("-")
                    .labelNames(labelNames)
                    .register();
            this.abortLatency =
                    this.buildSummary("pulsar_txn_tb_client_abort_latency", "-", labelNames);
            this.commitLatency =
                    this.buildSummary("pulsar_txn_tb_client_commit_latency", "-", labelNames);

            this.pendingRequests = GaugeWithCallback.builder()
                    .name("pulsar_txn_tb_client_pending_requests")
                    .help("-")
                    .callback(callback -> callback.call(handler.getPendingRequestsCount()))
                    .register();
        }

        private Summary buildSummary(String name, String help, String[] labelNames) {
            Summary.Builder builder = Summary.builder().name(name).help(help)
                    .labelNames(labelNames)
                    .maxAgeSeconds(QUANTILE_WINDOW_SECONDS);
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
                PrometheusRegistry.defaultRegistry.unregister(this.abortFailed);
                PrometheusRegistry.defaultRegistry.unregister(this.commitFailed);
                PrometheusRegistry.defaultRegistry.unregister(this.abortLatency);
                PrometheusRegistry.defaultRegistry.unregister(this.commitLatency);
                PrometheusRegistry.defaultRegistry.unregister(this.pendingRequests);
            }
        }
    }
}
