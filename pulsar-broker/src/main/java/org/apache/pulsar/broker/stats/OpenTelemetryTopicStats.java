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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryTopicStats implements AutoCloseable {

    public static final String SUBSCRIPTION_COUNTER = "pulsar_subscriptions_count";
    private final ObservableLongMeasurement subscriptionCounter;

    public static final String PRODUCER_COUNTER = "pulsar_producers_count";
    private final ObservableLongMeasurement producerCounter;

    public static final String CONSUMER_COUNTER = "pulsar_consumers_count";
    private final ObservableLongMeasurement consumerCounter;

    public static final String MESSAGE_IN_COUNTER = "pulsar_rate_in";
    private final ObservableLongMeasurement messageInCounter;

    public static final String MESSAGE_OUT_COUNTER = "pulsar_rate_out";
    private final ObservableLongMeasurement messageOutCounter;

    // Omitted: pulsar_publish_rate_limit_times

    public static final String BYTES_IN_COUNTER = "pulsar_throughput_in";
    private final ObservableLongMeasurement bytesInCounter;

    public static final String BYTES_OUT_COUNTER = "pulsar_throughput_out";
    private final ObservableLongMeasurement bytesOutCounter;

    public static final String CONSUMER_MSG_ACK_COUNTER = "pulsar_consumer_msg_ack_rate";
    private final ObservableLongMeasurement consumerMsgAckCounter;

    public static final String STORAGE_COUNTER = "pulsar_storage_size";
    private final ObservableLongMeasurement storageCounter;

    public static final String STORAGE_LOGICAL_COUNTER = "pulsar_storage_logical_size";
    private final ObservableLongMeasurement storageLogicalCounter;

    public static final String STORAGE_BACKLOG_COUNTER = "pulsar_storage_backlog_size";
    private final ObservableLongMeasurement storageBacklogCounter;

    public static final String STORAGE_OFFLOADED_COUNTER = "pulsar_storage_offloaded_size";
    private final ObservableLongMeasurement storageOffloadedCounter;

    // Omitted: pulsar_storage_backlog_quota_limit

    // Omitted: pulsar_storage_backlog_age_seconds

    // Omitted: pulsar_storage_backlog_quota_exceeded_evictions_total

    // Omitted: pulsar_storage_write_rate

    // Omitted: pulsar_storage_read_rate

    public static final String DELAYED_SUBSCRIPTION_COUNTER = "pulsar_subscription_delayed";
    private final ObservableLongMeasurement delayedSubscriptionCounter;

    // Omitted: pulsar_storage_write_latency_le_*

    // Omitted: pulsar_entry_size_le_*

    // Omitted: pulsar_in_bytes_total

    // Omitted: pulsar_in_messages_total

    // Omitted: pulsar_out_bytes_total

    // Omitted: pulsar_out_messages_total

    public static final String COMPACTION_REMOVED_COUNTED = "pulsar_compaction_removed_event_count";
    private final ObservableLongMeasurement compactionRemovedCounted;

    public static final String COMPACTION_SUCCEEDED_COUNTER = "pulsar_compaction_succeed_count";
    private final ObservableLongMeasurement compactionSucceededCounter;

    public static final String COMPACTION_FAILED_COUNTER = "pulsar_compaction_failed_count";
    private final ObservableLongMeasurement compactionFailedCounter;

    // Omitted: pulsar_compaction_duration_time_in_mills

    // Omitted: pulsar_compaction_read_throughput

    // Omitted: pulsar_compaction_write_throughput

    // Omitted: pulsar_compaction_latency_le_*

    // Omitted: pulsar_compaction_compacted_entries_count

    // Omitted: pulsar_compaction_compacted_entries_size

    public static final String TRANSACTION_ACTIVE_COUNTER = "pulsar_txn_tb_active_total";
    private final ObservableLongMeasurement transactionActiveCounter;

    public static final String TRANSACTION_ABORTED_COUNTER = "pulsar_txn_tb_aborted_total";
    private final ObservableLongMeasurement transactionAbortedCounter;

    public static final String TRANSACTION_COMMITTED_COUNTER = "pulsar_txn_tb_committed_total";
    private final ObservableLongMeasurement transactionCommittedCounter;

    // Omitted: pulsar_delayed_message_index_size_bytes

    // Omitted: pulsar_delayed_message_index_bucket_total

    // Omitted: pulsar_delayed_message_index_loaded

    // Omitted: pulsar_delayed_message_index_bucket_snapshot_size_bytes

    // Omitted: pulsar_delayed_message_index_bucket_op_count

    // Omitted: pulsar_delayed_message_index_bucket_op_latency_ms


    private final BatchCallback batchCallback;
    private final GetStatsOptions getStatsOptions;
    private final PulsarService pulsar;

    public OpenTelemetryTopicStats(PulsarService pulsar) {
        this.pulsar = pulsar;
        var meter = pulsar.getOpenTelemetry().getMeter();

        subscriptionCounter = meter
                .upDownCounterBuilder(SUBSCRIPTION_COUNTER)
                .setUnit("{subscription}")
                .setDescription("The number of Pulsar subscriptions of the topic served by this broker.")
                .buildObserver();

        producerCounter = meter
                .upDownCounterBuilder(PRODUCER_COUNTER)
                .setUnit("{producer}")
                .setDescription("The number of active producers of the topic connected to this broker.")
                .buildObserver();

        consumerCounter = meter
                .upDownCounterBuilder(CONSUMER_COUNTER)
                .setUnit("{consumer}")
                .setDescription("The number of active consumers of the topic connected to this broker.")
                .buildObserver();

        messageInCounter = meter
                .upDownCounterBuilder(MESSAGE_IN_COUNTER)
                .setUnit("{message}")
                .setDescription("The total message rate of the topic coming into this broker (message per second).")
                .buildObserver();

        messageOutCounter = meter
                .upDownCounterBuilder(MESSAGE_OUT_COUNTER)
                .setUnit("{message}")
                .setDescription("The total message rate of the topic going out from this broker (message per second).")
                .buildObserver();

        bytesInCounter = meter
                .upDownCounterBuilder(BYTES_IN_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total throughput of the topic coming into this broker (byte per second).")
                .buildObserver();

        bytesOutCounter = meter
                .upDownCounterBuilder(BYTES_OUT_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total throughput of the topic going out from this broker (byte per second).")
                .buildObserver();

        consumerMsgAckCounter = meter
                .upDownCounterBuilder(CONSUMER_MSG_ACK_COUNTER)
                .setUnit("{ack}")
                .setDescription("The total message acknowledgment rate of the topic connected to this broker (message per second).")
                .buildObserver();

        storageCounter = meter
                .upDownCounterBuilder(STORAGE_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total storage size of the topics in this topic owned by this broker (bytes).")
                .buildObserver();

        storageLogicalCounter = meter
                .upDownCounterBuilder(STORAGE_LOGICAL_COUNTER)
                .setUnit("{byte}")
                .setDescription("The storage size of topics in the namespace owned by the broker without replicas (in bytes).")
                .buildObserver();

        storageBacklogCounter = meter
                .upDownCounterBuilder(STORAGE_BACKLOG_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total backlog size of the topics of this topic owned by this broker (in bytes).")
                .buildObserver();

        storageOffloadedCounter = meter
                .upDownCounterBuilder(STORAGE_OFFLOADED_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total amount of the data in this topic offloaded to the tiered storage (bytes).")
                .buildObserver();

        delayedSubscriptionCounter = meter
                .upDownCounterBuilder(DELAYED_SUBSCRIPTION_COUNTER)
                .setUnit("{subscription}")
                .setDescription("The total message batches (entries) are delayed for dispatching.")
                .buildObserver();

        compactionRemovedCounted = meter
                .upDownCounterBuilder(COMPACTION_REMOVED_COUNTED)
                .setUnit("{event}")
                .setDescription("The total number of removed events of the compaction.")
                .buildObserver();

        compactionSucceededCounter = meter
                .upDownCounterBuilder(COMPACTION_SUCCEEDED_COUNTER)
                .setUnit("{event}")
                .setDescription("The total number of successes of the compaction.")
                .buildObserver();

        compactionFailedCounter = meter
                .upDownCounterBuilder(COMPACTION_FAILED_COUNTER)
                .setUnit("{event}")
                .setDescription("The total number of failures of the compaction.")
                .buildObserver();

        transactionActiveCounter = meter
                .upDownCounterBuilder(TRANSACTION_ACTIVE_COUNTER)
                .setUnit("{transaction}")
                .setDescription("The number of active transactions on this topic.")
                .buildObserver();

        transactionAbortedCounter = meter
                .upDownCounterBuilder(TRANSACTION_ABORTED_COUNTER)
                .setUnit("{transaction}")
                .setDescription("The number of aborted transactions on the topic.")
                .buildObserver();

        transactionCommittedCounter = meter
                .upDownCounterBuilder(TRANSACTION_COMMITTED_COUNTER)
                .setUnit("{transaction}")
                .setDescription("The number of committed transactions on the topic.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .map(topicFuture -> topicFuture.getNow(Optional.empty()))
                        .forEach(topic -> topic.ifPresent(this::recordMetricsForTopic)),
                subscriptionCounter,
                producerCounter,
                consumerCounter,
                messageInCounter,
                messageOutCounter,
                bytesInCounter,
                bytesOutCounter,
                consumerMsgAckCounter,
                storageCounter,
                storageLogicalCounter,
                storageBacklogCounter,
                storageOffloadedCounter,
                delayedSubscriptionCounter,
                compactionRemovedCounted,
                compactionSucceededCounter,
                compactionFailedCounter,
                transactionActiveCounter,
                transactionAbortedCounter,
                transactionCommittedCounter);

        getStatsOptions = GetStatsOptions.builder()
                .getPreciseBacklog(true)
                .subscriptionBacklogSize(true)
                .getEarliestTimeInBacklog(true)
                .excludePublishers(false)
                .excludeConsumers(false)
                .build();
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForTopic(Topic topic) {
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic.getName())
                .build();

        var dummyValue = 0L;
        var topicStatsImpl = topic.getStats(getStatsOptions);

        subscriptionCounter.record(dummyValue, attributes);
        producerCounter.record(dummyValue, attributes);
        consumerCounter.record(dummyValue, attributes);
        messageInCounter.record(dummyValue, attributes);
        messageOutCounter.record(dummyValue, attributes);
        bytesInCounter.record(dummyValue, attributes);
        bytesOutCounter.record(dummyValue, attributes);
        consumerMsgAckCounter.record(dummyValue, attributes);
        storageCounter.record(dummyValue, attributes);
        storageLogicalCounter.record(dummyValue, attributes);
        storageBacklogCounter.record(dummyValue, attributes);
        storageOffloadedCounter.record(dummyValue, attributes);
        delayedSubscriptionCounter.record(dummyValue, attributes);
        compactionRemovedCounted.record(dummyValue, attributes);
        compactionSucceededCounter.record(dummyValue, attributes);
        compactionFailedCounter.record(dummyValue, attributes);
        transactionActiveCounter.record(dummyValue, attributes);
        transactionAbortedCounter.record(dummyValue, attributes);
        transactionCommittedCounter.record(dummyValue, attributes);

    }
}
