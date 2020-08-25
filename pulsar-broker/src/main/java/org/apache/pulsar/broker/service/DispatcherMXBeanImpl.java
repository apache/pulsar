/**
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
package org.apache.pulsar.broker.service;

import org.apache.bookkeeper.mledger.impl.EntryCacheCounter;
import org.apache.pulsar.common.stats.Rate;

import java.util.concurrent.TimeUnit;

public class DispatcherMXBeanImpl implements DispatcherMXBean {
    private AbstractBaseDispatcher dispatcher;

    private final Rate cacheHits = new Rate();
    private final Rate cacheMisses = new Rate();

    public DispatcherMXBeanImpl(AbstractBaseDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void refreshStats(long period, TimeUnit unit) {
        double seconds = unit.toMillis(period) / 1000.0;

        if (seconds <= 0.0) {
            // skip refreshing stats
            return;
        }

        cacheHits.calculateRate(seconds);
        cacheMisses.calculateRate(seconds);
    }

    public void recordCacheState(EntryCacheCounter entryCacheCounter) {
        if (entryCacheCounter != null) {
            if (entryCacheCounter.getCacheHitCount() > 0 && entryCacheCounter.getCacheHitSize() > 0) {
                recordCacheHits(entryCacheCounter.getCacheHitCount(), entryCacheCounter.getCacheHitSize());
            }
            if (entryCacheCounter.getCacheMissCount() > 0 && entryCacheCounter.getCacheMissSize() > 0) {
                recordCacheMiss(entryCacheCounter.getCacheMissCount(), entryCacheCounter.getCacheMissSize());
            }
        }
    }

    public void recordCacheHit(long size) {
        cacheHits.recordEvent(size);
    }

    private void recordCacheHits(int count, long totalSize) {
        cacheHits.recordMultipleEvents(count, totalSize);
    }

    private void recordCacheMiss(int count, long totalSize) {
        cacheMisses.recordMultipleEvents(count, totalSize);
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
}