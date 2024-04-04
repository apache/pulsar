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
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.stats.MetricsUtil;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryTopicStats implements AutoCloseable {

    // Replaces pulsar_subscriptions_count
    public static final String SUBSCRIPTION_COUNTER = "pulsar.broker.topic.subscription.count";
    private final ObservableLongMeasurement subscriptionCounter;

    // Replaces pulsar_producers_count
    public static final String PRODUCER_COUNTER = "pulsar.broker.topic.producer.count";
    private final ObservableLongMeasurement producerCounter;

    // Replaces pulsar_consumers_count
    public static final String CONSUMER_COUNTER = "pulsar.broker.topic.consumer.count";
    private final ObservableLongMeasurement consumerCounter;

    // Replaces ['pulsar_rate_in', 'pulsar_in_messages_total']
    public static final String MESSAGE_IN_COUNTER = "pulsar.broker.topic.message.incoming.count";
    private final ObservableLongMeasurement messageInCounter;

    // Replaces ['pulsar_rate_out', 'pulsar_out_messages_total']
    public static final String MESSAGE_OUT_COUNTER = "pulsar.broker.topic.message.outgoing.count";
    private final ObservableLongMeasurement messageOutCounter;

    // Replaces ['pulsar_throughput_in', 'pulsar_in_bytes_total']
    public static final String BYTES_IN_COUNTER = "pulsar.broker.topic.message.incoming.size";
    private final ObservableLongMeasurement bytesInCounter;

    // Replaces ['pulsar_throughput_out', 'pulsar_out_bytes_total']
    public static final String BYTES_OUT_COUNTER = "pulsar.broker.topic.message.outgoing.size";
    private final ObservableLongMeasurement bytesOutCounter;

    // Replaces pulsar_publish_rate_limit_times
    public static final String PUBLISH_RATE_LIMIT_HIT_COUNTER = "pulsar.broker.topic.publish.rate.limit";
    private final ObservableLongMeasurement publishRateLimitHitCounter;

    // Replaces pulsar_consumer_msg_ack_rate
    public static final String CONSUMER_MSG_ACK_COUNTER = "pulsar.broker.topic.consumer.msg.ack";
    private final ObservableLongMeasurement consumerMsgAckCounter;

    // Replaces pulsar_storage_size
    public static final String STORAGE_COUNTER = "pulsar.broker.topic.storage.size";
    private final ObservableLongMeasurement storageCounter;

    // Replaces pulsar_storage_logical_size
    public static final String STORAGE_LOGICAL_COUNTER = "pulsar.broker.topic.storage.logical.size";
    private final ObservableLongMeasurement storageLogicalCounter;

    // Replaces pulsar_storage_backlog_size
    public static final String STORAGE_BACKLOG_COUNTER = "pulsar.broker.topic.storage.backlog.size";
    private final ObservableLongMeasurement storageBacklogCounter;

    // Replaces pulsar_storage_offloaded_size
    public static final String STORAGE_OFFLOADED_COUNTER = "pulsar.broker.topic.storage.offloaded.size";
    private final ObservableLongMeasurement storageOffloadedCounter;

    // Replaces pulsar_storage_backlog_quota_limit
    public static final String BACKLOG_QUOTA_LIMIT = "pulsar.broker.topic.storage.backlog.quota.limit";
    private final ObservableLongMeasurement backlogQuotaLimit;

    // Replaces pulsar_storage_backlog_quota_exceeded_evictions_total
    public static final String BACKLOG_EVICTION_COUNTER = "pulsar.broker.topic.storage.backlog.quota.exceeded.eviction.count";
    private final ObservableLongMeasurement backlogEvictionCounter;

    // Replaces pulsar_storage_backlog_age_seconds
    public static final String BACKLOG_QUOTA_AGE = "pulsar.broker.topic.storage.backlog.age";
    private final ObservableLongMeasurement backlogQuotaAge;

    // Replaces pulsar_storage_write_rate
    public static final String STORAGE_OUT_COUNTER = "pulsar.broker.topic.storage.outgoing";
    private final ObservableLongMeasurement storageOutCounter;

    // Replaces pulsar_storage_read_rate
    public static final String STORAGE_IN_COUNTER = "pulsar.broker.topic.storage.incoming";
    private final ObservableLongMeasurement storageInCounter;

    // Omitted: pulsar_storage_write_latency_le_*

    // Omitted: pulsar_entry_size_le_*

    // Replaces pulsar_compaction_removed_event_count
    public static final String COMPACTION_REMOVED_COUNTED = "pulsar.broker.topic.compaction.removed.event.count";
    private final ObservableLongMeasurement compactionRemovedCounted;

    // Replaces pulsar_compaction_succeed_count
    public static final String COMPACTION_SUCCEEDED_COUNTER = "pulsar.broker.topic.compaction.succeed.count";
    private final ObservableLongMeasurement compactionSucceededCounter;

    // Replaces pulsar_compaction_failed_count
    public static final String COMPACTION_FAILED_COUNTER = "pulsar.broker.topic.compaction.failed.count";
    private final ObservableLongMeasurement compactionFailedCounter;

    // Replaces pulsar_compaction_duration_time_in_mills
    public static final String COMPACTION_DURATION_SECONDS = "pulsar.broker.topic.compaction.duration";
    private final ObservableDoubleMeasurement compactionDurationSeconds;

    // Replaces pulsar_compaction_read_throughput
    public static final String COMPACTION_BYTES_IN_COUNTER = "pulsar.broker.topic.compaction.incoming";
    private final ObservableLongMeasurement compactionBytesInCounter;

    // Replaces pulsar_compaction_write_throughput
    public static final String COMPACTION_BYTES_OUT_COUNTER = "pulsar.broker.topic.compaction.outgoing";
    private final ObservableLongMeasurement compactionBytesOutCounter;

    // Omitted: pulsar_compaction_latency_le_*

    // Replaces pulsar_compaction_compacted_entries_count
    public static final String COMPACTION_ENTRIES_COUNTER = "pulsar.broker.topic.compaction.compacted.entries.count";
    private final ObservableLongMeasurement compactionEntriesCounter;

    // Replaces pulsar_compaction_compacted_entries_size
    public static final String COMPACTION_BYTES_COUNTER = "pulsar.broker.topic.compaction.compacted.entries.size";
    private final ObservableLongMeasurement compactionBytesCounter;

    // Replaces ['pulsar_txn_tb_active_total', 'pulsar_txn_tb_aborted_total', 'pulsar_txn_tb_committed_total']
    public static final String TRANSACTION_COUNTER = "pulsar.broker.topic.transaction";
    private final ObservableLongMeasurement transactionCounter;

    // Replaces pulsar_subscription_delayed
    public static final String DELAYED_SUBSCRIPTION_COUNTER = "pulsar.broker.topic.subscription.delayed";
    private final ObservableLongMeasurement delayedSubscriptionCounter;

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
                .setDescription("The total number of messages received for this topic.")
                .buildObserver();

        messageOutCounter = meter
                .upDownCounterBuilder(MESSAGE_OUT_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages read from this topic.")
                .buildObserver();

        bytesInCounter = meter
                .upDownCounterBuilder(BYTES_IN_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total number of messages bytes received for this topic.")
                .buildObserver();

        bytesOutCounter = meter
                .upDownCounterBuilder(BYTES_OUT_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total number of messages bytes read from this topic.")
                .buildObserver();

        publishRateLimitHitCounter = meter
                .upDownCounterBuilder(PUBLISH_RATE_LIMIT_HIT_COUNTER)
                .setUnit("{event}")
                .setDescription("The number of times the publish rate limit is triggered.")
                .buildObserver();

        consumerMsgAckCounter = meter
                .upDownCounterBuilder(CONSUMER_MSG_ACK_COUNTER)
                .setUnit("{ack}")
                .setDescription("The total number of message acknowledgments received for this topic.")
                .buildObserver();

        storageCounter = meter
                .upDownCounterBuilder(STORAGE_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total storage size of the messages in this topic.")
                .buildObserver();

        storageLogicalCounter = meter
                .upDownCounterBuilder(STORAGE_LOGICAL_COUNTER)
                .setUnit("{byte}")
                .setDescription("The storage size of topics in the namespace owned by the broker without replicas.")
                .buildObserver();

        storageBacklogCounter = meter
                .upDownCounterBuilder(STORAGE_BACKLOG_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total backlog size of the topics of this topic owned by this broker.")
                .buildObserver();

        storageOffloadedCounter = meter
                .upDownCounterBuilder(STORAGE_OFFLOADED_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total amount of the data in this topic offloaded to the tiered storage (bytes).")
                .buildObserver();

        backlogQuotaLimit = meter
                .upDownCounterBuilder(BACKLOG_QUOTA_LIMIT)
                .setUnit("{byte}")
                .setDescription("The total amount of the data in this topic that limit the backlog quota.")
                .buildObserver();

        backlogEvictionCounter = meter
                .upDownCounterBuilder(BACKLOG_EVICTION_COUNTER)
                .setUnit("{eviction}")
                .setDescription("The number of times a backlog was evicted since it has exceeded its quota. FIXME Includes attribute quota_type = (time|size).")
                .buildObserver();

        backlogQuotaAge = meter
                .upDownCounterBuilder(BACKLOG_QUOTA_AGE)
                .setUnit("{second}")
                .setDescription("The age of the oldest unacknowledged message (backlog).")
                .buildObserver();

        storageOutCounter = meter
                .upDownCounterBuilder(STORAGE_OUT_COUNTER)
                .setUnit("{message batch}")
                .setDescription("The total message batches (entries) written to the storage for this topic.")
                .buildObserver();

        storageInCounter = meter
                .upDownCounterBuilder(STORAGE_IN_COUNTER)
                .setUnit("{message batch}")
                .setDescription("The total message batches (entries) read from the storage for this topic.")
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

        compactionDurationSeconds = meter
                .upDownCounterBuilder(COMPACTION_DURATION_SECONDS)
                .ofDoubles()
                .setUnit("{second}")
                .setDescription("The total time duration of compaction operations on the topic.")
                .buildObserver();

        compactionBytesInCounter = meter
                .upDownCounterBuilder(COMPACTION_BYTES_IN_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total count of bytes read by the compaction process for this topic.")
                .buildObserver();

        compactionBytesOutCounter = meter
                .upDownCounterBuilder(COMPACTION_BYTES_OUT_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total count of bytes written by the compaction process for this topic.")
                .buildObserver();

        compactionEntriesCounter = meter
                .upDownCounterBuilder(COMPACTION_ENTRIES_COUNTER)
                .setUnit("{entry}")
                .setDescription("The total number of the compacted entries.")
                .buildObserver();

        compactionBytesCounter = meter
                .upDownCounterBuilder(COMPACTION_BYTES_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total size of the compacted entries.")
                .buildObserver();

        transactionCounter = meter
                .upDownCounterBuilder(TRANSACTION_COUNTER)
                .setUnit("{transaction}")
                .setDescription("The number of transactions on this topic.")
                .buildObserver();

        delayedSubscriptionCounter = meter
                .upDownCounterBuilder(DELAYED_SUBSCRIPTION_COUNTER)
                .setUnit("{message batch}")
                .setDescription("The total message batches (entries) are delayed for dispatching.")
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
                publishRateLimitHitCounter,
                consumerMsgAckCounter,
                storageCounter,
                storageLogicalCounter,
                storageBacklogCounter,
                storageOffloadedCounter,
                backlogQuotaLimit,
                backlogEvictionCounter,
                backlogQuotaAge,
                storageOutCounter,
                storageInCounter,
                compactionRemovedCounted,
                compactionSucceededCounter,
                compactionFailedCounter,
                compactionDurationSeconds,
                compactionBytesInCounter,
                compactionBytesOutCounter,
                compactionEntriesCounter,
                compactionBytesCounter,
                transactionCounter,
                delayedSubscriptionCounter);

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

        if (topic instanceof AbstractTopic abstractTopic) {
            subscriptionCounter.record(abstractTopic.getSubscriptions().size(), attributes);
            producerCounter.record(abstractTopic.getProducers().size(), attributes);
            consumerCounter.record(abstractTopic.getNumberOfConsumers(), attributes);

            messageInCounter.record(abstractTopic.getMsgInCounter(), attributes);
            messageOutCounter.record(abstractTopic.getMsgOutCounter(), attributes);
            bytesInCounter.record(abstractTopic.getBytesInCounter(), attributes);
            bytesOutCounter.record(abstractTopic.getBytesOutCounter(), attributes);

            publishRateLimitHitCounter.record(abstractTopic.getTotalPublishRateLimitCounter(), attributes);
        }

        consumerMsgAckCounter.record(dummyValue, attributes); // FIXME: not implemented?

        if (topic instanceof PersistentTopic persistentTopic) {
            var delayedMessages = topic.getSubscriptions().values().stream()
                    .map(Subscription::getDispatcher)
                    .mapToLong(Dispatcher::getNumberOfDelayedMessages)
                    .sum();
            delayedSubscriptionCounter.record(delayedMessages, attributes);

            var managedLedger = persistentTopic.getManagedLedger();
            var managedLedgerStats = persistentTopic.getManagedLedger().getStats();
            storageCounter.record(managedLedgerStats.getStoredMessagesSize(), attributes);
            storageLogicalCounter.record(managedLedgerStats.getStoredMessagesLogicalSize(), attributes);
            storageBacklogCounter.record(managedLedger.getEstimatedBacklogSize(), attributes);
            storageOffloadedCounter.record(managedLedger.getOffloadedSize(), attributes);
            storageInCounter.record(managedLedgerStats.getReadEntriesSucceededTotal(), attributes);
            storageOutCounter.record(managedLedgerStats.getAddEntrySucceedTotal(), attributes);

            backlogQuotaLimit.record(
                    topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage).getLimitSize(),
                    attributes);
            var backlogQuotaLimitTime = topic
                    .getBacklogQuota(BacklogQuota.BacklogQuotaType.message_age).getLimitTime();
            backlogQuotaAge.record(topic.getBestEffortOldestUnacknowledgedMessageAgeSeconds(), attributes);
            var backlogQuotaMetrics = persistentTopic.getPersistentTopicMetrics().getBacklogQuotaMetrics();
            backlogEvictionCounter.record(backlogQuotaMetrics.getSizeBasedBacklogQuotaExceededEvictionCount(),
                    Attributes.builder()
                            .putAll(attributes)
                            .put(OpenTelemetryAttributes.PULSAR_BACKLOG_QUOTA_TYPE, "size")
                            .build());
            backlogEvictionCounter.record(backlogQuotaMetrics.getTimeBasedBacklogQuotaExceededEvictionCount(),
                    Attributes.builder()
                            .putAll(attributes)
                            .put(OpenTelemetryAttributes.PULSAR_BACKLOG_QUOTA_TYPE, "time")
                            .build());

            var txnBuffer = persistentTopic.getTransactionBuffer();
            transactionCounter.record(txnBuffer.getOngoingTxnCount(), Attributes.builder()
                    .putAll(attributes)
                    .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "active")
                    .build());
            transactionCounter.record(txnBuffer.getCommittedTxnCount(), Attributes.builder()
                    .putAll(attributes)
                    .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "committed")
                    .build());
            transactionCounter.record(txnBuffer.getAbortedTxnCount(), Attributes.builder()
                    .putAll(attributes)
                    .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "aborted")
                    .build());

            Optional.ofNullable(pulsar.getNullableCompactor())
                    .map(Compactor::getStats)
                    .flatMap(compactorMXBean -> compactorMXBean.getCompactionRecordForTopic(topic.getName()))
                    .ifPresent(compactionRecord -> {
                        compactionRemovedCounted.record(compactionRecord.getCompactionRemovedEventCount(), attributes);
                        compactionSucceededCounter.record(compactionRecord.getCompactionSucceedCount(), attributes);
                        compactionFailedCounter.record(compactionRecord.getCompactionFailedCount(), attributes);
                        compactionDurationSeconds.record(MetricsUtil.convertToSeconds(
                            compactionRecord.getCompactionDurationTimeInMills(), TimeUnit.MILLISECONDS), attributes);
                        compactionBytesInCounter.record(compactionRecord.readRate.getTotalCount(), attributes);
                        compactionBytesOutCounter.record(compactionRecord.writeRate.getTotalCount(), attributes);

                        persistentTopic.getCompactedTopicContext().map(CompactedTopicContext::getLedger)
                                .ifPresent(ledger -> {
                                    compactionEntriesCounter.record(ledger.getLastAddConfirmed() + 1, attributes);
                                    compactionBytesCounter.record(ledger.getLength(), attributes);
                                });
                    });
        }
    }
}
