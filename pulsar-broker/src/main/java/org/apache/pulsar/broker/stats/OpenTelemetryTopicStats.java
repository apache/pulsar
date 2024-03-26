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
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryTopicStats implements AutoCloseable {

    private static final String INSTRUMENT_PREFIX = "pulsar.broker.messaging.topic.";
    public static final String CONSUMER_COUNTER = INSTRUMENT_PREFIX + "consumer";
    public static final String PRODUCER_COUNTER = INSTRUMENT_PREFIX + "producer";

    public static final String SUBSCRIPTION_COUNTER = INSTRUMENT_PREFIX + "subscription";
    private final ObservableLongMeasurement subscriptionCounter;
    private final ObservableLongMeasurement producerCounter;
    private final ObservableLongMeasurement consumerCounter;
    private final ObservableLongMeasurement messageIncomingCounter;
    private final ObservableLongMeasurement messageOutgoingCounter;
    private final ObservableLongMeasurement publishRateLimitCounter;
    private final ObservableLongMeasurement bytesIncomingCounter;
    private final ObservableLongMeasurement bytesOutgoingCounter;
    private final ObservableLongMeasurement storageCounter;
    private final ObservableLongMeasurement activeTransactionCounter;
    private final ObservableLongMeasurement committedTransactionCounter;
    private final ObservableLongMeasurement abortedTransactionCounter;

    private final BatchCallback batchCallback;

    private final GetStatsOptions getStatsOptions;

    private final PulsarService pulsar;

    public OpenTelemetryTopicStats(PulsarService pulsar) {
        this.pulsar = pulsar;
        var meter = pulsar.getOpenTelemetry().getMeter();

        subscriptionCounter = meter
                .upDownCounterBuilder(SUBSCRIPTION_COUNTER)
                .setUnit("{subscription}")
                .buildObserver();

        producerCounter = meter
                .upDownCounterBuilder(PRODUCER_COUNTER)
                .setUnit("{producer}")
                .buildObserver();

        consumerCounter = meter
                .upDownCounterBuilder(CONSUMER_COUNTER)
                .setUnit("{consumer}")
                .buildObserver();

        messageIncomingCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "message.incoming")
                .setUnit("{message}")
                .buildObserver();

        messageOutgoingCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "message.outgoing")
                .setUnit("{message}")
                .buildObserver();

        publishRateLimitCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "publish.rate.limit.exceeded")
                .setUnit("{operation}")
                .buildObserver();

        bytesIncomingCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "byte.incoming")
                .setUnit("{byte}")
                .buildObserver();

        bytesOutgoingCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "byte.outgoing")
                .setUnit("{byte}")
                .buildObserver();

        storageCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "storage")
                .setUnit("{byte}")
                .buildObserver();

        // Compaction (persistent topic only).
        meter.counterBuilder(INSTRUMENT_PREFIX + "compaction.removed.event.count")
                .setDescription("The total number of removed events of the compaction.")
                .setUnit("{event}")
                .buildObserver();
        meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_removed_event_count")
                .setDescription(" The total number of removed events of the compaction.")
                .setUnit("{event}")
                .buildObserver();
        meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_succeed_count")
                .setDescription(" The total number of successes of the compaction.")
                .buildObserver();
        meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_failed_count")
                .setDescription(" The total number of failures of the compaction.")
                .buildObserver();
        meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_duration_time_in_mills")
                .setDescription(" The duration time of the compaction.")
                .buildObserver();
        meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_read_throughput")
                .setDescription(" The read throughput of the compaction.")
                .buildObserver();
        meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_write_throughput")
                .setDescription(" The write throughput of the compaction.")
                .buildObserver();
        meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_compacted_entries_count")
                .setDescription(" The total number of the compacted entries.")
                .buildObserver();
        meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_compacted_entries_size")
                .setDescription(" The total size of the compacted entries.")
                .buildObserver();

        // Transaction metrics (persistent topic only).
        activeTransactionCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "active_transaction")
                .setDescription("The number of active transactions on the topic.")
                .setUnit("{transaction}")
                .buildObserver();
        committedTransactionCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "committed_transaction")
                .setDescription("The number of committed transactions on the topic.")
                .setUnit("{transaction}")
                .buildObserver();
        abortedTransactionCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "aborted_transaction")
                .setDescription("The number of aborted transactions on the topic.")
                .setUnit("{transaction}")
                .buildObserver();
        /*
         meter.counterBuilder(INSTRUMENT_PREFIX + "pulsar_compaction_latency_le_*                          | Histogram | The compaction latency with given quantile. <br /> Available thresholds: <br /><ul><li>pulsar_compaction_latency_le_0_5: <= 0.5ms </li><li>pulsar_compaction_latency_le_1: <= 1ms</li><li>pulsar_compaction_latency_le_5: <= 5ms</li><li>pulsar_compaction_latency_le_10: <= 10ms</li><li>pulsar_compaction_latency_le_20: <= 20ms</li><li>pulsar_compaction_latency_le_50: <= 50ms</li><li>pulsar_compaction_latency_le_100: <= 100ms</li><li>pulsar_compaction_latency_le_200: <= 200ms</li><li>pulsar_compaction_latency_le_1000: <= 1s</li><li>pulsar_compaction_latency_le_overflow: > 1s</li></ul>                                                                                                                                 |
         */
        batchCallback = meter.batchCallback(() -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .map(topicFuture -> topicFuture.getNow(Optional.empty()))
                        .forEach(topic -> topic.ifPresent(this::recordMetricsForTopic)),
                subscriptionCounter,
                producerCounter,
                consumerCounter,
                messageIncomingCounter,
                messageOutgoingCounter,
                publishRateLimitCounter);

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
        subscriptionCounter.record(100, attributes);

        var topicStatsImpl = topic.getStats(getStatsOptions);

        storageCounter.record(100, Attributes.builder()
                .putAll(attributes)
                .put(OpenTelemetryAttributes.PULSAR_STORAGE_TYPE, "logical")
                .build());

        storageCounter.record(100, Attributes.builder()
                .putAll(attributes)
                .put(OpenTelemetryAttributes.PULSAR_STORAGE_TYPE, "backlog")
                .build());

        storageCounter.record(100, Attributes.builder()
                .putAll(attributes)
                .put(OpenTelemetryAttributes.PULSAR_STORAGE_TYPE, "offloaded")
                .build());

        subscriptionCounter.record(topicStatsImpl.getSubscriptions().size(), attributes);
        producerCounter.record(topicStatsImpl.getPublishers().size(), attributes);
        messageIncomingCounter.record(topicStatsImpl.msgInCounter, attributes);
        messageOutgoingCounter.record(topicStatsImpl.msgOutCounter, attributes);

        if (topic instanceof PersistentTopic persistentTopic) {
            activeTransactionCounter.record(topicStatsImpl.getOngoingTxnCount(), attributes);
            committedTransactionCounter.record(topicStatsImpl.getCommittedTxnCount(), attributes);
            abortedTransactionCounter.record(topicStatsImpl.getAbortedTxnCount(), attributes);
        }

        if (topic.isPersistent() && pulsar.getNullableCompactor() != null) {
            pulsar.getNullableCompactor().getStats().getCompactionRecordForTopic(topic.getName()).ifPresent(
                    compactionRecord -> {
                        compactionRecord.writeRate.getTotalCount();
                    });
        }

        topicStatsImpl.getAbortedTxnCount();

        /*
        | pulsar_throughput_in                                    | Gauge     | The total throughput of the topic coming into this broker (byte per second).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
        | pulsar_throughput_out                                   | Gauge     | The total throughput of the topic going out from this broker (byte per second).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

        | pulsar_consumer_msg_ack_rate                            | Gauge     | The total message acknowledgment rate of the topic connected to this broker (message per second).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

        | pulsar_storage_size                                     | Gauge     | The total storage size of the topics in this topic owned by this broker (bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
        | pulsar_storage_logical_size                             | Gauge     | The storage size of topics in the namespace owned by the broker without replicas (in bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
        | pulsar_storage_backlog_size                             | Gauge     | The total backlog size of the topics of this topic owned by this broker (in bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
        | pulsar_storage_offloaded_size                           | Gauge     | The total amount of the data in this topic offloaded to the tiered storage (bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
        | pulsar_storage_backlog_quota_limit                      | Gauge     | The total amount of the data in this topic that limit the backlog quota (bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
        | pulsar_storage_backlog_age_seconds                      | Gauge     | The age of the oldest unacknowledged message (backlog).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
        | pulsar_storage_backlog_quota_exceeded_evictions_total   | Counter   | The number of times a backlog was evicted since it has exceeded its quota. Includes label `quota_type = (time \| size)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
        | pulsar_storage_write_rate                               | Gauge     | The total message batches (entries) written to the storage for this topic (message batch per second).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
        | pulsar_storage_read_rate                                | Gauge     | The total message batches (entries) read from the storage for this topic (message batch per second).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

        | pulsar_subscription_delayed                             | Gauge     | The total message batches (entries) are delayed for dispatching.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
        | pulsar_storage_write_latency_le_*                       | Histogram | The entry rate of a topic that the storage write latency is smaller with a given threshold.<br /> Available thresholds: <br /><ul><li>pulsar_storage_write_latency_le_0_5: <= 0.5ms </li><li>pulsar_storage_write_latency_le_1: <= 1ms</li><li>pulsar_storage_write_latency_le_5: <= 5ms</li><li>pulsar_storage_write_latency_le_10: <= 10ms</li><li>pulsar_storage_write_latency_le_20: <= 20ms</li><li>pulsar_storage_write_latency_le_50: <= 50ms</li><li>pulsar_storage_write_latency_le_100: <= 100ms</li><li>pulsar_storage_write_latency_le_200: <= 200ms</li><li>pulsar_storage_write_latency_le_1000: <= 1s</li><li>pulsar_storage_write_latency_le_overflow: > 1s</li></ul>                                                    |
        | pulsar_entry_size_le_*                                  | Histogram | The entry rate of a topic that the entry size is smaller with a given threshold.<br /> Available thresholds: <br /><ul><li>pulsar_entry_size_le_128: <= 128 bytes </li><li>pulsar_entry_size_le_512: <= 512 bytes</li><li>pulsar_entry_size_le_1_kb: <= 1 KB</li><li>pulsar_entry_size_le_2_kb: <= 2 KB</li><li>pulsar_entry_size_le_4_kb: <= 4 KB</li><li>pulsar_entry_size_le_16_kb: <= 16 KB</li><li>pulsar_entry_size_le_100_kb: <= 100 KB</li><li>pulsar_entry_size_le_1_mb: <= 1 MB</li><li>pulsar_entry_size_le_overflow: > 1 MB</li></ul>                                                                                                                                                                                        |

        | pulsar_in_bytes_total                                   | Counter   | The total number of messages in bytes received for this topic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
        | pulsar_in_messages_total                                | Counter   | The total number of messages received for this topic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
        | pulsar_out_bytes_total                                  | Counter   | The total number of messages in bytes read from this topic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
        | pulsar_out_messages_total                               | Counter   | The total number of messages read from this topic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

        | pulsar_delayed_message_index_size_bytes                 | Gauge     | The total memory size allocated by `DelayedDeliveryTracker` of the topic owned by this broker (in bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
        | pulsar_delayed_message_index_bucket_total               | Gauge     | The number of delayed message index buckets (immutable buckets + LastMutableBucket )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
        | pulsar_delayed_message_index_loaded                     | Gauge     | The total number of delayed message indexes for in the memory.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
        | pulsar_delayed_message_index_bucket_snapshot_size_bytes | Gauge     | The total size of delayed message index bucket snapshot (in bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
        | pulsar_delayed_message_index_bucket_op_count            | Counter   | The total number of operation delayed message index bucket snapshots. The `state` label can be `succeed`,`failed`, and`all` (`all` means the total number of all states) and the `type` label can be `create`,`load`,`delete`, and `merge`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
        | pulsar_delayed_message_index_bucket_op_latency_ms       | Histogram | The latency of delayed message index bucket snapshot operation with a given quantile (threshold). The label`type` label can be `create`,`load`,`delete`, and `merge`<br/>The label `quantile` can be:<ul><li>quantile="50" is operation latency between (0ms, 50ms]</li><li>quantile="100" is operation latency between (50ms, 100ms]</li><li>quantile="500" is operation latency between (100ms, 500ms]</li><li>quantile="1000" is operation latency between (500ms, 1s]</li><li>quantile="5000" is operation latency between (1s, 5s]</li><li>quantile="30000" is operation latency between (5s, 30s]</li><li>quantile="60000" is operation latency between (30s, 60s]</li><li>quantile="overflow" is operation latency > 1m</li></ul> |
        */
    }

}
