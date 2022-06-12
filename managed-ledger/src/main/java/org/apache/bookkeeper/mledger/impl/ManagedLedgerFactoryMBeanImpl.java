/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import lombok.Data;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.stats.Rate;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("checkstyle:javadoctype")
public class ManagedLedgerFactoryMBeanImpl implements ManagedLedgerFactoryMXBean {

    private final Gauge mlCount =
            Gauge.build("pulsar_ml_count", "-").register();
    private final Gauge mlCacheUsedSize =
            Gauge.build("pulsar_ml_cache_used_size", "-").register();
    private final Gauge mlCacheEvictions =
            Gauge.build("pulsar_ml_cache_evictions", "-").register();
    private final Gauge brkMlCacheHitsRate =
            Gauge.build("pulsar_ml_cache_hits_rate", "-").register();
    private final Gauge mlCacheMissesRate =
            Gauge.build("pulsar_ml_cache_misses_rate", "-").register();
    private final Gauge mlCacheHitsThroughput =
            Gauge.build("pulsar_ml_cache_hits_throughput", "-").register();
    private final Gauge mlCacheMissesThroughput =
            Gauge.build("pulsar_ml_cache_misses_throughput", "-").register();

    //Cache pool metrics
    private final Gauge cachePoolUsed =
            Gauge.build("pulsar_ml_cache_pool_used", "-").register();
    private final Gauge cachePoolAllocated =
            Gauge.build("pulsar_ml_cache_pool_allocated", "-").register();
    private final Gauge cachePoolActiveAllocations =
            Gauge.build("pulsar_ml_cache_pool_active_allocations", "-").register();
    private final Gauge cachePoolActiveAllocationsSmall =
            Gauge.build("pulsar_ml_cache_pool_active_allocations_small", "-").register();
    private final Gauge cachePoolActiveAllocationsNormal =
            Gauge.build("pulsar_ml_cache_pool_active_allocations_normal", "-").register();
    private final Gauge cachePoolActiveAllocationsHuge =
            Gauge.build("pulsar_ml_cache_pool_active_allocations_huge", "-").register();

    private final ManagedLedgerFactoryImpl factory;
    private final long periodInMillis;

    final Rate cacheHits = new Rate();
    final Rate cacheMisses = new Rate();
    final Rate cacheEvictions = new Rate();


    public ManagedLedgerFactoryMBeanImpl(ManagedLedgerFactoryImpl factory, int periodInSeconds) {
        this.factory = factory;
        this.periodInMillis = periodInSeconds * 1000L;
        initialize();
    }

    public void refreshStats(long period, TimeUnit unit) {
        double seconds = unit.toMillis(period) / 1000.0;

        if (seconds <= 0.0) {
            // skip refreshing stats
            return;
        }

        cacheHits.calculateRate(seconds);
        cacheMisses.calculateRate(seconds);
        cacheEvictions.calculateRate(seconds);
    }

    public void recordCacheHit(long size) {
        cacheHits.recordEvent(size);
    }

    public void recordCacheHits(int count, long totalSize) {
        cacheHits.recordMultipleEvents(count, totalSize);
    }

    public void recordCacheMiss(int count, long totalSize) {
        cacheMisses.recordMultipleEvents(count, totalSize);
    }

    public void recordCacheEviction() {
        cacheEvictions.recordEvent();
    }

    @Override
    public int getNumberOfManagedLedgers() {
        return factory.ledgers.size();
    }

    @Override
    public long getCacheUsedSize() {
        return factory.getEntryCacheManager().getSize();
    }

    @Override
    public long getCacheMaxSize() {
        return factory.getEntryCacheManager().getMaxSize();
    }

    @Override
    public double getCacheHitsRate() {
        return cacheHits.getRate();
    }

    @Override
    public double getCacheMissesRate() {
        return cacheMisses.getRate();
    }

    @Override
    public double getCacheHitsThroughput() {
        return cacheHits.getValueRate();
    }

    @Override
    public double getCacheMissesThroughput() {
        return cacheMisses.getValueRate();
    }

    @Override
    public long getNumberOfCacheEvictions() {
        return cacheEvictions.getCount();
    }

    @Override
    public long getCachePoolUsed() {
        return getMetrics().totalUsed;
    }

    @Override
    public long getCachePoolAllocated() {
        return getMetrics().totalAllocated;
    }

    @Override
    public long getCachePoolActiveAllocations() {
        return getMetrics().activeAllocations;
    }

    @Override
    public long getCachePoolActiveAllocationsSmall() {
        return getMetrics().activeAllocationsSmall;
    }

    @Override
    public long getCachePoolActiveAllocationsNormal() {
        return getMetrics().activeAllocationsNormal;
    }

    @Override
    public long getCachePoolActiveAllocationsHuge() {
        return getMetrics().activeAllocationsHuge;
    }

    private void initialize() {
        this.mlCount.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getNumberOfManagedLedgers();
            }
        });
        this.mlCacheUsedSize.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getCacheUsedSize();
            }
        });
        this.mlCacheEvictions.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getNumberOfCacheEvictions();
            }
        });
        this.brkMlCacheHitsRate.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getCacheHitsRate();
            }
        });
        this.mlCacheMissesRate.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getCacheMissesRate();
            }
        });

        this.mlCacheHitsThroughput.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getCacheHitsThroughput();
            }
        });
        this.mlCacheMissesThroughput.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getCacheMissesThroughput();
            }
        });


        //cache pool metrics
        cachePoolUsed.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getMetrics().totalUsed;
            }
        });
        cachePoolAllocated.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getMetrics().totalAllocated;
            }
        });
        cachePoolActiveAllocations.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getMetrics().activeAllocations;
            }
        });
        cachePoolActiveAllocationsSmall.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getMetrics().activeAllocationsSmall;
            }
        });
        cachePoolActiveAllocationsNormal.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getMetrics().activeAllocationsNormal;
            }
        });
        cachePoolActiveAllocationsHuge.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getMetrics().activeAllocationsHuge;
            }
        });
    }

    public void close() {
        CollectorRegistry.defaultRegistry.register(mlCount);
        CollectorRegistry.defaultRegistry.register(mlCacheUsedSize);
        CollectorRegistry.defaultRegistry.register(mlCacheEvictions);
        CollectorRegistry.defaultRegistry.register(brkMlCacheHitsRate);
        CollectorRegistry.defaultRegistry.register(mlCacheMissesRate);
        CollectorRegistry.defaultRegistry.register(mlCacheHitsThroughput);
        CollectorRegistry.defaultRegistry.register(mlCacheMissesThroughput);

        CollectorRegistry.defaultRegistry.register(cachePoolUsed);
        CollectorRegistry.defaultRegistry.register(cachePoolAllocated);
        CollectorRegistry.defaultRegistry.register(cachePoolActiveAllocations);
        CollectorRegistry.defaultRegistry.register(cachePoolActiveAllocationsSmall);
        CollectorRegistry.defaultRegistry.register(cachePoolActiveAllocationsNormal);
        CollectorRegistry.defaultRegistry.register(cachePoolActiveAllocationsHuge);
    }


    private synchronized BufferPoolMetrics getMetrics() {
        long now = System.currentTimeMillis();
        long start = now - now % periodInMillis;

        BufferPoolMetrics instance = BufferPoolMetrics.reference.get();

        if (instance == null || instance.start < start) {
            BufferPoolMetrics metrics = new BufferPoolMetrics(start);
            PooledByteBufAllocator allocator = RangeEntryCacheImpl.ALLOCATOR;
            for (PoolArenaMetric arena : allocator.metric().directArenas()) {
                metrics.activeAllocations += arena.numActiveAllocations();
                metrics.activeAllocationsSmall += arena.numActiveSmallAllocations();
                metrics.activeAllocationsNormal += arena.numActiveNormalAllocations();
                metrics.activeAllocationsHuge += arena.numActiveHugeAllocations();

                for (PoolChunkListMetric list : arena.chunkLists()) {
                    for (PoolChunkMetric chunk : list) {
                        int size = chunk.chunkSize();
                        int used = size - chunk.freeBytes();

                        metrics.totalAllocated += size;
                        metrics.totalUsed += used;
                    }
                }
            }
            BufferPoolMetrics.reference.set(metrics);
        }

        return instance;
    }

    @Data
    static class BufferPoolMetrics {
        private long activeAllocations = 0;
        private long activeAllocationsSmall = 0;
        private long activeAllocationsNormal = 0;
        private long activeAllocationsHuge = 0;
        private long totalAllocated = 0;
        private long totalUsed = 0;

        private final long start;

        private BufferPoolMetrics(long start) {
            this.start = start;
        }

        private static AtomicReference<BufferPoolMetrics> reference;
    }


    public Collection<Metrics> generate() {
        Metrics m = new Metrics();

        m.put("brk_ml_count", getNumberOfManagedLedgers());
        m.put("brk_ml_cache_used_size", getCacheUsedSize());
        m.put("brk_ml_cache_evictions", getNumberOfCacheEvictions());
        m.put("brk_ml_cache_hits_rate", getCacheHitsRate());
        m.put("brk_ml_cache_misses_rate", getCacheMissesRate());
        m.put("brk_ml_cache_hits_throughput", getCacheHitsThroughput());
        m.put("brk_ml_cache_misses_throughput", getCacheMissesThroughput());

        BufferPoolMetrics bufferPoolMetrics = getMetrics();
        m.put("brk_ml_cache_pool_allocated", bufferPoolMetrics.totalAllocated);
        m.put("brk_ml_cache_pool_used", bufferPoolMetrics.totalUsed);
        m.put("brk_ml_cache_pool_active_allocations", bufferPoolMetrics.activeAllocations);
        m.put("brk_ml_cache_pool_active_allocations_small", bufferPoolMetrics.activeAllocationsSmall);
        m.put("brk_ml_cache_pool_active_allocations_normal", bufferPoolMetrics.activeAllocationsNormal);
        m.put("brk_ml_cache_pool_active_allocations_huge", bufferPoolMetrics.activeAllocationsHuge);

        return Collections.singletonList(m);
    }
}
