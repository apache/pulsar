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


import com.google.common.collect.Range;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LongPairRangeSetWrapper<T extends Comparable<T>> implements LongPairRangeSet<T> {

    private final TreeMap<Long, LongPairRangeSet<T>> ledgerIdToPosition = new TreeMap<>();
    final LongPairConsumer<T> positionRangeConverter;
    final ManagedLedgerConfig config;

    public LongPairRangeSetWrapper(ManagedLedgerConfig config, LongPairConsumer<T> positionRangeConverter) {
        this.positionRangeConverter = positionRangeConverter;
        this.config = config;
    }

    @Override
    public void addOpenClosed(long lowerKey, long lowerValue, long upperKey, long upperValue) {
        LongPairRangeSet<T> pairRangeSet = ledgerIdToPosition
                .computeIfAbsent(upperKey, (key) -> getLongPairRangeSetImpl());
        if (config.isEnableLruCacheMaxUnackedRanges()) {
            // lru switch
        } else {
            pairRangeSet.addOpenClosed(lowerKey, lowerValue, upperKey, upperValue);
        }
    }

    private LongPairRangeSet<T>  getLongPairRangeSetImpl() {
        return config.isUnackedRangesOpenCacheSetEnabled()
                ? new ConcurrentOpenLongPairRangeSet<>(4096, positionRangeConverter)
                : new LongPairRangeSet.DefaultRangeSet<>(positionRangeConverter);
    }

    @Override
    public boolean contains(long key, long value) {
        LongPairRangeSet<T> longPairRangeSet = ledgerIdToPosition.get(key);
        if (longPairRangeSet == null) {
            return false;
        }
        return longPairRangeSet.contains(key, value);
    }

    @Override
    public Range rangeContaining(long key, long value) {
        LongPairRangeSet<T> longPairRangeSet = ledgerIdToPosition.get(key);
        if (longPairRangeSet == null) {
            return null;
        }
        return longPairRangeSet.rangeContaining(key, value);
    }

    @Override
    public void removeAtMost(long key, long value) {
        LongPairRangeSet<T> longPairRangeSet = ledgerIdToPosition.get(key);
        if (longPairRangeSet == null) {
            return;
        }
        longPairRangeSet.removeAtMost(key, value);
    }

    @Override
    public boolean isEmpty() {
        return ledgerIdToPosition.isEmpty();
    }

    @Override
    public void clear() {
        ledgerIdToPosition.clear();
    }

    @Override
    public Range<T> span() {
        Range<T> rangeFirst = ledgerIdToPosition.firstEntry().getValue().span();
        Range<T> rangeLast = ledgerIdToPosition.lastEntry().getValue().span();
        return Range.openClosed(rangeFirst.lowerEndpoint(), rangeLast.upperEndpoint());
    }

    @Override
    public List<Range<T>> asRanges() {
        List<Range<T>> ranges = new ArrayList<>();
        ledgerIdToPosition.values().forEach(ledgerIdToPosition -> ranges.addAll(ledgerIdToPosition.asRanges()));
        return ranges;
    }

    @Override
    public void forEach(RangeProcessor<T> action) {
        ledgerIdToPosition.values().forEach(ledgerIdToPosition -> ledgerIdToPosition.forEach(action));
    }

    @Override
    public void forEach(RangeProcessor<T> action, LongPairConsumer<? extends T> consumer) {
        ledgerIdToPosition.values().forEach(ledgerIdToPosition -> ledgerIdToPosition.forEach(action, consumer));
    }

    @Override
    public int size() {
        AtomicInteger size = new AtomicInteger(0);
        ledgerIdToPosition.values().forEach(ledgerIdToPosition -> size.addAndGet(ledgerIdToPosition.size()));
        return size.get();
    }

    @Override
    public Range<T> firstRange() {
        Map.Entry<Long, LongPairRangeSet<T>> entry = ledgerIdToPosition.firstEntry();
        if (entry == null) {
            return null;
        }
        return entry.getValue().firstRange();
    }

    @Override
    public Range<T> lastRange() {
        Map.Entry<Long, LongPairRangeSet<T>> entry = ledgerIdToPosition.lastEntry();
        if (entry == null) {
            return null;
        }
        return entry.getValue().lastRange();
    }
}
