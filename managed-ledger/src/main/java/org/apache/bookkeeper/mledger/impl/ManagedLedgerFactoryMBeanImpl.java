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
package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.pulsar.common.stats.Rate;

@SuppressWarnings("checkstyle:javadoctype")
public class ManagedLedgerFactoryMBeanImpl implements ManagedLedgerFactoryMXBean {

    private final ManagedLedgerFactoryImpl factory;

    final Rate cacheHits = new Rate();
    final Rate cacheMisses = new Rate();
    final Rate cacheEvictions = new Rate();

    private final LongAdder insertedEntryCount = new LongAdder();
    private final LongAdder evictedEntryCount = new LongAdder();
    private final LongAdder cacheEntryCount = new LongAdder();

    public ManagedLedgerFactoryMBeanImpl(ManagedLedgerFactoryImpl factory) throws Exception {
        this.factory = factory;
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

    public void recordCacheInsertion() {
        insertedEntryCount.increment();
        cacheEntryCount.increment();
    }

    public void recordNumberOfCacheEntriesEvicted(int count) {
        evictedEntryCount.add(count);
        cacheEntryCount.add(-count);
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

    public long getCacheInsertedEntriesCount() {
        return insertedEntryCount.sum();
    }

    public long getCacheEvictedEntriesCount() {
        return evictedEntryCount.sum();
    }

    public long getCacheEntriesCount() {
        return cacheEntryCount.sum();
    }

}
