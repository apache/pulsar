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
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryNamespaceStats implements AutoCloseable {

    // Replaces pulsar_topics_count
    public static final String TOPIC_COUNTER = "pulsar.broker.namespace.topic.count";
    private final ObservableLongMeasurement topicCounter;

    // Replaces pulsar_subscriptions_count
    public static final String SUBSCRIPTION_COUNTER = "pulsar.broker.namespace.subscription.count";
    private final ObservableLongMeasurement subscriptionCounter;

    // Replaces pulsar_producers_count
    public static final String PRODUCER_COUNTER = "pulsar.broker.namespace.producer.count";
    private final ObservableLongMeasurement producerCounter;

    // Replaces pulsar_consumers_count
    public static final String CONSUMER_COUNTER = "pulsar.broker.namespace.consumer.count";
    private final ObservableLongMeasurement consumerCounter;

    // Replaces ['pulsar_rate_in', 'pulsar_in_messages_total']
    public static final String MESSAGE_IN_COUNTER = "pulsar.broker.namespace.message.incoming.count";
    private final ObservableLongMeasurement messageInCounter;

    // Replaces ['pulsar_rate_out', 'pulsar_out_messages_total']
    public static final String MESSAGE_OUT_COUNTER = "pulsar.broker.namespace.message.outgoing.count";
    private final ObservableLongMeasurement messageOutCounter;

    // Replaces ['pulsar_throughput_in', 'pulsar_in_bytes_total']
    public static final String BYTES_IN_COUNTER = "pulsar.broker.namespace.message.incoming.size";
    private final ObservableLongMeasurement bytesInCounter;

    // Replaces ['pulsar_throughput_out', 'pulsar_out_bytes_total']
    public static final String BYTES_OUT_COUNTER = "pulsar.broker.namespace.message.outgoing.size";
    private final ObservableLongMeasurement bytesOutCounter;

    // Replaces pulsar_publish_rate_limit_times
    public static final String PUBLISH_RATE_LIMIT_HIT_COUNTER = "pulsar.broker.namespace.publish.rate.limit.count";
    private final ObservableLongMeasurement publishRateLimitHitCounter;

    // Omitted: pulsar_consumer_msg_ack_rate

    // Replaces pulsar_storage_size
    public static final String STORAGE_COUNTER = "pulsar.broker.namespace.storage.size";
    private final ObservableLongMeasurement storageCounter;

    // Replaces pulsar_storage_logical_size
    public static final String STORAGE_LOGICAL_COUNTER = "pulsar.broker.namespace.storage.logical.size";
    private final ObservableLongMeasurement storageLogicalCounter;

    // Replaces pulsar_storage_backlog_size
    public static final String STORAGE_BACKLOG_COUNTER = "pulsar.broker.namespace.storage.backlog.size";
    private final ObservableLongMeasurement storageBacklogCounter;

    // Replaces pulsar_storage_offloaded_size
    public static final String STORAGE_OFFLOADED_COUNTER = "pulsar.broker.namespace.storage.offloaded.size";
    private final ObservableLongMeasurement storageOffloadedCounter;

    // Replaces pulsar_storage_backlog_quota_limit
    public static final String BACKLOG_QUOTA_LIMIT_SIZE = "pulsar.broker.namespace.storage.backlog.quota.limit.size";
    private final ObservableLongMeasurement backlogQuotaLimitSize;

    // Replaces pulsar_storage_backlog_quota_limit_time
    public static final String BACKLOG_QUOTA_LIMIT_TIME = "pulsar.broker.namespace.storage.backlog.quota.limit.time";
    private final ObservableLongMeasurement backlogQuotaLimitTime;

    // Replaces pulsar_storage_backlog_quota_exceeded_evictions_total
    public static final String BACKLOG_EVICTION_COUNTER = "pulsar.broker.namespace.storage.backlog.quota.eviction.count";
    private final ObservableLongMeasurement backlogEvictionCounter;

    // Replaces pulsar_storage_backlog_age_seconds
    public static final String BACKLOG_QUOTA_AGE = "pulsar.broker.namespace.storage.backlog.age";
    private final ObservableLongMeasurement backlogQuotaAge;

    // Replaces pulsar_storage_write_rate
    public static final String STORAGE_OUT_COUNTER = "pulsar.broker.namespace.storage.entry.outgoing.count";
    private final ObservableLongMeasurement storageOutCounter;

    // Replaces pulsar_storage_read_rate
    public static final String STORAGE_IN_COUNTER = "pulsar.broker.namespace.storage.entry.incoming.count";
    private final ObservableLongMeasurement storageInCounter;

    // Omitted: pulsar_storage_write_latency_le_*

    // Omitted: pulsar_entry_size_le_*

    // Omitted: pulsar_compaction_latency_le_*

    // Replaces pulsar_subscription_delayed
    public static final String DELAYED_SUBSCRIPTION_COUNTER =
            "pulsar.broker.namespace.subscription.delayed.entry.count";
    private final ObservableLongMeasurement delayedSubscriptionCounter;

    // Omitted: pulsar_delayed_message_index_size_bytes

    // Omitted: pulsar_delayed_message_index_bucket_total

    // Omitted: pulsar_delayed_message_index_loaded

    // Omitted: pulsar_delayed_message_index_bucket_snapshot_size_bytes

    // Omitted: pulsar_delayed_message_index_bucket_op_count

    // Omitted: pulsar_delayed_message_index_bucket_op_latency_ms


    private final BatchCallback batchCallback;
    private final PulsarService pulsar;

    public OpenTelemetryNamespaceStats(PulsarService pulsar) {
        this.pulsar = pulsar;
        var meter = pulsar.getOpenTelemetry().getMeter();

        topicCounter = meter
                .upDownCounterBuilder(TOPIC_COUNTER)
                .setUnit("{topic}")
                .setDescription("The number of Pulsar topics of the namespace served by this broker.")
                .buildObserver();

        subscriptionCounter = meter
                .upDownCounterBuilder(SUBSCRIPTION_COUNTER)
                .setUnit("{subscription}")
                .setDescription("The number of Pulsar subscriptions of the namespace served by this broker.")
                .buildObserver();

        producerCounter = meter
                .upDownCounterBuilder(PRODUCER_COUNTER)
                .setUnit("{producer}")
                .setDescription("The number of active producers of the namespace connected to this broker.")
                .buildObserver();

        consumerCounter = meter
                .upDownCounterBuilder(CONSUMER_COUNTER)
                .setUnit("{consumer}")
                .setDescription("The number of active consumers of the namespace connected to this broker.")
                .buildObserver();

        messageInCounter = meter
                .counterBuilder(MESSAGE_IN_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages received for topics in this namespace.")
                .buildObserver();

        messageOutCounter = meter
                .counterBuilder(MESSAGE_OUT_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages read from this topics in this namespace.")
                .buildObserver();

        bytesInCounter = meter
                .counterBuilder(BYTES_IN_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes received for topics in this namespace.")
                .buildObserver();

        bytesOutCounter = meter
                .counterBuilder(BYTES_OUT_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes read from topics in this namespace.")
                .buildObserver();

        publishRateLimitHitCounter = meter
                .counterBuilder(PUBLISH_RATE_LIMIT_HIT_COUNTER)
                .setUnit("{event}")
                .setDescription("The number of times the publish rate limit is triggered.")
                .buildObserver();

        storageCounter = meter
                .upDownCounterBuilder(STORAGE_COUNTER)
                .setUnit("By")
                .setDescription("The total storage size of the messages in this namespace, including storage used by replicas.")
                .buildObserver();

        storageLogicalCounter = meter
                .upDownCounterBuilder(STORAGE_LOGICAL_COUNTER)
                .setUnit("By")
                .setDescription("The storage size of the messages in this namespace, excluding storage used by replicas.")
                .buildObserver();

        storageBacklogCounter = meter
                .upDownCounterBuilder(STORAGE_BACKLOG_COUNTER)
                .setUnit("By")
                .setDescription("The size of the backlog storage for this namespace.")
                .buildObserver();

        storageOffloadedCounter = meter
                .upDownCounterBuilder(STORAGE_OFFLOADED_COUNTER)
                .setUnit("By")
                .setDescription("The total amount of the data in this namespace offloaded to the tiered storage.")
                .buildObserver();

        backlogQuotaLimitSize = meter
                .upDownCounterBuilder(BACKLOG_QUOTA_LIMIT_SIZE)
                .setUnit("By")
                .setDescription("The size based backlog quota limit for this namespace.")
                .buildObserver();

        backlogQuotaLimitTime = meter
                .gaugeBuilder(BACKLOG_QUOTA_LIMIT_TIME)
                .ofLongs()
                .setUnit("s")
                .setDescription("The time based backlog quota limit for this namespace.")
                .buildObserver();

        backlogEvictionCounter = meter
                .counterBuilder(BACKLOG_EVICTION_COUNTER)
                .setUnit("{eviction}")
                .setDescription("The number of times a backlog was evicted since it has exceeded its quota.")
                .buildObserver();

        backlogQuotaAge = meter
                .gaugeBuilder(BACKLOG_QUOTA_AGE)
                .ofLongs()
                .setUnit("s")
                .setDescription("The age of the oldest unacknowledged message (backlog).")
                .buildObserver();

        storageOutCounter = meter
                .counterBuilder(STORAGE_OUT_COUNTER)
                .setUnit("{entry}")
                .setDescription("The total message batches (entries) written to the storage for this topic.")
                .buildObserver();

        storageInCounter = meter
                .counterBuilder(STORAGE_IN_COUNTER)
                .setUnit("{entry}")
                .setDescription("The total message batches (entries) read from the storage for this topic.")
                .buildObserver();

        delayedSubscriptionCounter = meter
                .upDownCounterBuilder(DELAYED_SUBSCRIPTION_COUNTER)
                .setUnit("{entry}")
                .setDescription("The total number of message batches (entries) delayed for dispatching.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .map(topicFuture -> topicFuture.getNow(Optional.empty()))
                        .forEach(topic -> topic.ifPresent(this::recordMetricsForTopic)),
                topicCounter,
                subscriptionCounter,
                producerCounter,
                consumerCounter,
                messageInCounter,
                messageOutCounter,
                bytesInCounter,
                bytesOutCounter,
                publishRateLimitHitCounter,
                storageCounter,
                storageLogicalCounter,
                storageBacklogCounter,
                storageOffloadedCounter,
                backlogQuotaLimitSize,
                backlogQuotaLimitTime,
                backlogEvictionCounter,
                backlogQuotaAge,
                storageOutCounter,
                storageInCounter,
                delayedSubscriptionCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForTopic(Topic topic) {
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic.getName())
                .build();

        var dummyValue = 1;

        topicCounter.record(dummyValue, attributes);
        subscriptionCounter.record(dummyValue, attributes);
        producerCounter.record(dummyValue, attributes);
        consumerCounter.record(dummyValue, attributes);
        messageInCounter.record(dummyValue, attributes);
        messageOutCounter.record(dummyValue, attributes);
        bytesInCounter.record(dummyValue, attributes);
        bytesOutCounter.record(dummyValue, attributes);
        publishRateLimitHitCounter.record(dummyValue, attributes);
        storageCounter.record(dummyValue, attributes);
        storageLogicalCounter.record(dummyValue, attributes);
        storageBacklogCounter.record(dummyValue, attributes);
        storageOffloadedCounter.record(dummyValue, attributes);
        backlogQuotaLimitSize.record(dummyValue, attributes);
        backlogQuotaLimitTime.record(dummyValue, attributes);
        backlogEvictionCounter.record(dummyValue, attributes);
        backlogQuotaAge.record(dummyValue, attributes);
        storageOutCounter.record(dummyValue, attributes);
        storageInCounter.record(dummyValue, attributes);
        delayedSubscriptionCounter.record(dummyValue, attributes);
    }
}