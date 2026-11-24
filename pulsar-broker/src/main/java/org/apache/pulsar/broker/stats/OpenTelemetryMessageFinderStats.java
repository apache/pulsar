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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.MetricsUtil;

/**
 * Metrics for the timestamp-based position search used by seek/reset-cursor by timestamp and by message TTL.
 *
 * <p>Attributes are intentionally limited to low-cardinality values (no topic or subscription), since a broker
 * may own a very large number of topics. Per-search details are logged by the finder instead.
 */
public class OpenTelemetryMessageFinderStats {

    public static final AttributeKey<String> FIND_REASON =
            AttributeKey.stringKey("pulsar.broker.message.find.reason");
    public enum FindReason {
        SEEK,
        EXPIRY;
        private final Attributes attributes = Attributes.of(FIND_REASON, name().toLowerCase(Locale.ROOT));
    }

    public static final AttributeKey<String> FIND_RESULT =
            AttributeKey.stringKey("pulsar.broker.message.find.result");
    public enum FindResult {
        FOUND,
        NOT_FOUND,
        FAILURE;
    }

    public static final AttributeKey<String> ENTRY_STORAGE =
            AttributeKey.stringKey("pulsar.broker.message.find.entry.storage");
    public enum EntryStorage {
        BOOKKEEPER,
        OFFLOADED;
    }

    public static final String FIND_DURATION_METRIC_NAME = "pulsar.broker.message.find.duration";
    private final DoubleHistogram findDuration;

    public static final String FIND_ENTRY_READ_COUNT_METRIC_NAME = "pulsar.broker.message.find.entry.read.count";
    private final LongCounter entryReadCounter;

    public static final String FIND_ENTRY_READ_SIZE_METRIC_NAME = "pulsar.broker.message.find.entry.read.size";
    private final LongCounter entryReadSizeCounter;

    private final Attributes[][] resultAttributes;
    private final Attributes[][] storageAttributes;

    public OpenTelemetryMessageFinderStats(PulsarService pulsar) {
        this(pulsar.getOpenTelemetry().getMeter());
    }

    public OpenTelemetryMessageFinderStats(Meter meter) {
        findDuration = meter.histogramBuilder(FIND_DURATION_METRIC_NAME)
                .setDescription("Time taken to find the position of a message by timestamp")
                .setUnit("s")
                .setExplicitBucketBoundariesAdvice(Arrays.asList(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
                        1.0, 2.5, 5.0, 10.0, 30.0, 60.0))
                .build();
        entryReadCounter = meter.counterBuilder(FIND_ENTRY_READ_COUNT_METRIC_NAME)
                .setDescription("The number of entries read while finding the position of a message by timestamp, "
                        + "by the storage of their ledger. Entries served from the broker entry cache are included.")
                .setUnit("{entry}")
                .build();
        entryReadSizeCounter = meter.counterBuilder(FIND_ENTRY_READ_SIZE_METRIC_NAME)
                .setDescription("The number of bytes read while finding the position of a message by timestamp, "
                        + "by the storage of their ledger. Entries served from the broker entry cache are included.")
                .setUnit("By")
                .build();

        FindReason[] reasons = FindReason.values();
        resultAttributes = new Attributes[reasons.length][FindResult.values().length];
        storageAttributes = new Attributes[reasons.length][EntryStorage.values().length];
        for (FindReason reason : reasons) {
            for (FindResult result : FindResult.values()) {
                resultAttributes[reason.ordinal()][result.ordinal()] = reason.attributes.toBuilder()
                        .put(FIND_RESULT, result.name().toLowerCase(Locale.ROOT))
                        .build();
            }
            for (EntryStorage storage : EntryStorage.values()) {
                storageAttributes[reason.ordinal()][storage.ordinal()] = reason.attributes.toBuilder()
                        .put(ENTRY_STORAGE, storage.name().toLowerCase(Locale.ROOT))
                        .build();
            }
        }
    }

    public void recordEntryRead(FindReason reason, EntryStorage storage, long sizeInBytes) {
        Attributes attributes = storageAttributes[reason.ordinal()][storage.ordinal()];
        entryReadCounter.add(1, attributes);
        entryReadSizeCounter.add(sizeInBytes, attributes);
    }

    public void recordFindCompleted(FindReason reason, FindResult result, long durationNanos) {
        findDuration.record(MetricsUtil.convertToSeconds(durationNanos, TimeUnit.NANOSECONDS),
                resultAttributes[reason.ordinal()][result.ordinal()]);
    }
}
