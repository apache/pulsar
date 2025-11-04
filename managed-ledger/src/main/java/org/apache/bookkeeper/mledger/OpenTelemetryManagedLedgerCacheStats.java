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
package org.apache.bookkeeper.mledger;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.cache.PooledByteBufAllocatorStats;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;
import org.apache.pulsar.opentelemetry.Constants;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.CacheEntryStatus;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.CacheOperationStatus;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.PoolArenaType;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.PoolChunkAllocationType;

public class OpenTelemetryManagedLedgerCacheStats implements AutoCloseable {

    // Replaces pulsar_ml_count
    public static final String MANAGED_LEDGER_COUNTER = "pulsar.broker.managed_ledger.count";
    private final ObservableLongMeasurement managedLedgerCounter;

    // Replaces pulsar_ml_cache_evictions
    public static final String CACHE_EVICTION_OPERATION_COUNTER = "pulsar.broker.managed_ledger.cache.eviction.count";
    private final ObservableLongMeasurement cacheEvictionOperationCounter;

    // Replaces 'pulsar_ml_cache_entries',
    //          'pulsar_ml_cache_inserted_entries_total',
    //          'pulsar_ml_cache_evicted_entries_total'
    public static final String CACHE_ENTRY_COUNTER = "pulsar.broker.managed_ledger.cache.entry.count";
    private final ObservableLongMeasurement cacheEntryCounter;

    // Replaces pulsar_ml_cache_used_size
    public static final String CACHE_SIZE_COUNTER = "pulsar.broker.managed_ledger.cache.entry.size";
    private final ObservableLongMeasurement cacheSizeCounter;

    // Replaces pulsar_ml_cache_hits_rate, pulsar_ml_cache_misses_rate
    public static final String CACHE_OPERATION_COUNTER = "pulsar.broker.managed_ledger.cache.operation.count";
    private final ObservableLongMeasurement cacheOperationCounter;

    // Replaces pulsar_ml_cache_hits_throughput, pulsar_ml_cache_misses_throughput
    public static final String CACHE_OPERATION_BYTES_COUNTER = "pulsar.broker.managed_ledger.cache.operation.size";
    private final ObservableLongMeasurement cacheOperationBytesCounter;

    // Replaces 'pulsar_ml_cache_pool_active_allocations',
    //          'pulsar_ml_cache_pool_active_allocations_huge',
    //          'pulsar_ml_cache_pool_active_allocations_normal',
    //          'pulsar_ml_cache_pool_active_allocations_small'
    public static final String CACHE_POOL_ACTIVE_ALLOCATION_COUNTER =
            "pulsar.broker.managed_ledger.cache.pool.allocation.active.count";
    private final ObservableLongMeasurement cachePoolActiveAllocationCounter;

    // Replaces ['pulsar_ml_cache_pool_allocated', 'pulsar_ml_cache_pool_used']
    public static final String CACHE_POOL_ACTIVE_ALLOCATION_SIZE_COUNTER =
            "pulsar.broker.managed_ledger.cache.pool.allocation.size";
    private final ObservableLongMeasurement cachePoolActiveAllocationSizeCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryManagedLedgerCacheStats(OpenTelemetry openTelemetry, ManagedLedgerFactoryImpl factory) {
        var meter = openTelemetry.getMeter(Constants.BROKER_INSTRUMENTATION_SCOPE_NAME);

        managedLedgerCounter = meter
                .upDownCounterBuilder(MANAGED_LEDGER_COUNTER)
                .setUnit("{managed_ledger}")
                .setDescription("The total number of managed ledgers.")
                .buildObserver();

        cacheEvictionOperationCounter = meter
                .counterBuilder(CACHE_EVICTION_OPERATION_COUNTER)
                .setUnit("{eviction}")
                .setDescription("The total number of cache eviction operations.")
                .buildObserver();

        cacheEntryCounter = meter
                .upDownCounterBuilder(CACHE_ENTRY_COUNTER)
                .setUnit("{entry}")
                .setDescription("The number of entries in the entry cache.")
                .buildObserver();

        cacheSizeCounter = meter
                .upDownCounterBuilder(CACHE_SIZE_COUNTER)
                .setUnit("{By}")
                .setDescription("The byte amount of entries stored in the entry cache.")
                .buildObserver();

        cacheOperationCounter = meter
                .counterBuilder(CACHE_OPERATION_COUNTER)
                .setUnit("{entry}")
                .setDescription("The number of cache operations.")
                .buildObserver();

        cacheOperationBytesCounter = meter
                .counterBuilder(CACHE_OPERATION_BYTES_COUNTER)
                .setUnit("{By}")
                .setDescription("The byte amount of data retrieved from cache operations.")
                .buildObserver();

        cachePoolActiveAllocationCounter = meter
                .upDownCounterBuilder(CACHE_POOL_ACTIVE_ALLOCATION_COUNTER)
                .setUnit("{allocation}")
                .setDescription("The number of currently active allocations in the direct arena.")
                .buildObserver();

        cachePoolActiveAllocationSizeCounter = meter
                .upDownCounterBuilder(CACHE_POOL_ACTIVE_ALLOCATION_SIZE_COUNTER)
                .setUnit("{By}")
                .setDescription("The memory allocated in the direct arena.")
                .buildObserver();


        batchCallback = meter.batchCallback(() -> recordMetrics(factory),
                managedLedgerCounter,
                cacheEvictionOperationCounter,
                cacheEntryCounter,
                cacheSizeCounter,
                cacheOperationCounter,
                cacheOperationBytesCounter,
                cachePoolActiveAllocationCounter,
                cachePoolActiveAllocationSizeCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetrics(ManagedLedgerFactoryImpl factory) {
        var stats = factory.getCacheStats();

        managedLedgerCounter.record(stats.getNumberOfManagedLedgers());
        cacheEvictionOperationCounter.record(stats.getNumberOfCacheEvictionsTotal());

        var entriesOut = stats.getCacheEvictedEntriesCount();
        var entriesIn = stats.getCacheInsertedEntriesCount();
        var entriesActive = entriesIn - entriesOut;
        cacheEntryCounter.record(entriesActive, CacheEntryStatus.ACTIVE.attributes);
        cacheEntryCounter.record(entriesIn, CacheEntryStatus.INSERTED.attributes);
        cacheEntryCounter.record(entriesOut, CacheEntryStatus.EVICTED.attributes);
        cacheSizeCounter.record(stats.getCacheUsedSize());

        cacheOperationCounter.record(stats.getCacheHitsTotal(), CacheOperationStatus.HIT.attributes);
        cacheOperationBytesCounter.record(stats.getCacheHitsBytesTotal(), CacheOperationStatus.HIT.attributes);
        cacheOperationCounter.record(stats.getCacheMissesTotal(), CacheOperationStatus.MISS.attributes);
        cacheOperationBytesCounter.record(stats.getCacheMissesBytesTotal(), CacheOperationStatus.MISS.attributes);

        var allocatorStats = new PooledByteBufAllocatorStats(RangeEntryCacheImpl.ALLOCATOR);
        cachePoolActiveAllocationCounter.record(allocatorStats.activeAllocationsSmall, PoolArenaType.SMALL.attributes);
        cachePoolActiveAllocationCounter.record(allocatorStats.activeAllocationsNormal,
                PoolArenaType.NORMAL.attributes);
        cachePoolActiveAllocationCounter.record(allocatorStats.activeAllocationsHuge, PoolArenaType.HUGE.attributes);
        cachePoolActiveAllocationSizeCounter.record(allocatorStats.totalAllocated,
                PoolChunkAllocationType.ALLOCATED.attributes);
        cachePoolActiveAllocationSizeCounter.record(allocatorStats.totalUsed, PoolChunkAllocationType.USED.attributes);
    }
}