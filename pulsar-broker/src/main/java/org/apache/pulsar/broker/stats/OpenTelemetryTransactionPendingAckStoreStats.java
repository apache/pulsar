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
package org.apache.pulsar.broker.stats;

import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;

public class OpenTelemetryTransactionPendingAckStoreStats implements AutoCloseable {

    // Replaces ['pulsar_txn_tp_committed_count_total', 'pulsar_txn_tp_aborted_count_total']
    public static final String ACK_COUNTER = "pulsar.broker.transaction.pending.ack.store.transaction.count";
    private final ObservableLongCounter ackCounter;

    public OpenTelemetryTransactionPendingAckStoreStats(PulsarService pulsar) {
        var meter = pulsar.getOpenTelemetry().getMeter();

        ackCounter = meter
                .counterBuilder(ACK_COUNTER)
                .setUnit("{transaction}")
                .setDescription("The number of transactions handled by the persistent ack store.")
                .buildWithCallback(measurement -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .filter(topicFuture -> topicFuture.isDone() && !topicFuture.isCompletedExceptionally())
                        .map(CompletableFuture::join)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(Topic::isPersistent)
                        .map(Topic::getSubscriptions)
                        .forEach(subs -> subs.forEach((__, sub) -> recordMetricsForSubscription(measurement, sub))));
    }

    @Override
    public void close() {
        ackCounter.close();
    }

    private void recordMetricsForSubscription(ObservableLongMeasurement measurement, Subscription subscription) {
        assert subscription instanceof PersistentSubscription; // The topics have already been filtered for persistence.
        var stats = ((PersistentSubscription) subscription).getPendingAckHandle().getPendingAckHandleStats();
        if (stats != null) {
            var attributes = stats.getAttributes();
            measurement.record(stats.getCommitSuccessCount(), attributes.getCommitSuccessAttributes());
            measurement.record(stats.getCommitFailedCount(), attributes.getCommitFailureAttributes());
            measurement.record(stats.getAbortSuccessCount(), attributes.getAbortSuccessAttributes());
            measurement.record(stats.getAbortFailedCount(), attributes.getAbortFailureAttributes());
        }
    }
}
