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
package org.apache.bookkeeper.mledger.impl;

import static java.util.Objects.requireNonNull;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;

/**
 * Wraps other Range classes, and adds LRU, marking dirty data and other features on this basis.
 * This range set is not thread safety.
 *
 * @param <T>
 */
public class RangeSetWrapper<T extends Comparable<T>> implements LongPairRangeSet<T> {

    private final LongPairRangeSet<T> rangeSet;
    private final LongPairConsumer<T> rangeConverter;
    private final ManagedLedgerConfig config;
    private final boolean enableMultiEntry;

    /**
     * Record which Ledger is dirty.
     */
    private final DefaultRangeSet<Long> dirtyLedgers = new LongPairRangeSet.DefaultRangeSet<>(
            (LongPairConsumer<Long>) (key, value) -> key,
            (RangeBoundConsumer<Long>) key -> new LongPair(key, 0));

    public RangeSetWrapper(LongPairConsumer<T> rangeConverter,
                           RangeBoundConsumer<T> rangeBoundConsumer,
                           ManagedCursorImpl managedCursor) {
        requireNonNull(managedCursor);
        this.config = managedCursor.getConfig();
        this.rangeConverter = rangeConverter;
        this.rangeSet = config.isUnackedRangesOpenCacheSetEnabled()
                ? new ConcurrentOpenLongPairRangeSet<>(4096, rangeConverter)
                : new LongPairRangeSet.DefaultRangeSet<>(rangeConverter, rangeBoundConsumer);
        this.enableMultiEntry = config.isPersistentUnackedRangesWithMultipleEntriesEnabled();
    }

    @Override
    public void addOpenClosed(long lowerKey, long lowerValue, long upperKey, long upperValue) {
        if (enableMultiEntry) {
            dirtyLedgers.addOpenClosed(lowerKey, 0, upperKey, 0);
        }
        rangeSet.addOpenClosed(lowerKey, lowerValue, upperKey, upperValue);
    }

    @Override
    public boolean contains(long key, long value) {
        return rangeSet.contains(key, value);
    }

    @Override
    public Range<T> rangeContaining(long key, long value) {
        return rangeSet.rangeContaining(key, value);
    }

    @Override
    public void removeAtMost(long key, long value) {
        if (enableMultiEntry) {
            dirtyLedgers.removeAtMost(key, 0);
        }
        rangeSet.removeAtMost(key, value);
    }

    @Override
    public boolean isEmpty() {
        return rangeSet.isEmpty();
    }

    @Override
    public void clear() {
        rangeSet.clear();
        dirtyLedgers.clear();
    }

    @Override
    public Range<T> span() {
        return rangeSet.span();
    }

    @Override
    public Collection<Range<T>> asRanges() {
        Collection<Range<T>> collection = rangeSet.asRanges();
        if (collection instanceof List) {
            return collection;
        }
        return new ArrayList<>(collection);
    }

    @Override
    public void forEach(RangeProcessor<T> action) {
        rangeSet.forEach(action);
    }

    @Override
    public void forEach(RangeProcessor<T> action, LongPairConsumer<? extends T> consumer) {
        rangeSet.forEach(action, consumer);
    }

    @Override
    public void forEachRawRange(RawRangeProcessor action) {
        rangeSet.forEachRawRange(action);
    }

    @Override
    public int size() {
        return rangeSet.size();
    }

    @Override
    public Range<T> firstRange() {
        return rangeSet.firstRange();
    }

    @Override
    public Range<T> lastRange() {
        return rangeSet.lastRange();
    }

    @Override
    public int cardinality(long lowerKey, long lowerValue, long upperKey, long upperValue) {
        return rangeSet.cardinality(lowerKey, lowerValue, upperKey, upperValue);
    }

    @VisibleForTesting
    void add(Range<LongPair> range) {
        if (!(rangeSet instanceof ConcurrentOpenLongPairRangeSet)) {
            throw new UnsupportedOperationException("Only ConcurrentOpenLongPairRangeSet support this method");
        }
        ((ConcurrentOpenLongPairRangeSet<T>) rangeSet).add(range);
    }

    @VisibleForTesting
    void remove(Range<T> range) {
        if (rangeSet instanceof ConcurrentOpenLongPairRangeSet) {
            ((ConcurrentOpenLongPairRangeSet<T>) rangeSet).remove((Range<LongPair>) range);
        } else {
            ((DefaultRangeSet<T>) rangeSet).remove(range);
        }
    }

    public void resetDirtyKeys() {
        dirtyLedgers.clear();
    }

    public boolean isDirtyLedgers(long ledgerId) {
        return dirtyLedgers.contains(ledgerId);
    }

    @Override
    public String toString() {
        return rangeSet.toString();
    }
}
