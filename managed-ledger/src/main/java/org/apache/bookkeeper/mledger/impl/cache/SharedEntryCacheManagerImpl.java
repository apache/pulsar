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
package org.apache.bookkeeper.mledger.impl.cache;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;

@Slf4j
public class SharedEntryCacheManagerImpl implements EntryCacheManager {

    private final ManagedLedgerFactoryConfig config;
    private final ManagedLedgerFactoryMBeanImpl factoryMBean;
    private final List<SharedCacheSegment> segments = new ArrayList<>();
    private int currentSegmentIdx = 0;
    private final int segmentSize;
    private final int segmentsCount;

    private final StampedLock lock = new StampedLock();

    private static final int DEFAULT_MAX_SEGMENT_SIZE = 1 * 1024 * 1024 * 1024;

    public SharedEntryCacheManagerImpl(ManagedLedgerFactoryImpl factory) {
        this.config = factory.getConfig();
        this.factoryMBean = factory.getMbean();
        long maxCacheSize = config.getMaxCacheSize();
        if (maxCacheSize > 0) {
            this.segmentsCount = Math.max(2, (int) (maxCacheSize / DEFAULT_MAX_SEGMENT_SIZE));
            this.segmentSize = (int) (maxCacheSize / segmentsCount);

            for (int i = 0; i < segmentsCount; i++) {
                if (config.isCopyEntriesInCache()) {
                    segments.add(new SharedCacheSegmentBufferCopy(segmentSize));
                } else {
                    segments.add(new SharedCacheSegmentBufferRefCount(segmentSize));
                }
            }
        } else {
            this.segmentsCount = 0;
            this.segmentSize = 0;
        }
    }

    ManagedLedgerFactoryMBeanImpl getFactoryMBean() {
        return factoryMBean;
    }

    @Override
    public EntryCache getEntryCache(ManagedLedgerImpl ml) {
        if (getMaxSize() > 0) {
            return new SharedEntryCacheImpl(ml, this);
        } else {
            return new EntryCacheDisabled(ml);
        }
    }

    @Override
    public void removeEntryCache(String name) {
        // no-op
    }

    @Override
    public long getSize() {
        long totalSize = 0;
        for (int i = 0; i < segmentsCount; i++) {
            totalSize += segments.get(i).getSize();
        }
        return totalSize;
    }

    @Override
    public long getMaxSize() {
        return config.getMaxCacheSize();
    }

    @Override
    public void clear() {
        segments.forEach(SharedCacheSegment::clear);
    }

    @Override
    public void close() {
        segments.forEach(SharedCacheSegment::close);
    }

    @Override
    public void updateCacheSizeAndThreshold(long maxSize) {

    }

    @Override
    public void updateCacheEvictionWatermark(double cacheEvictionWatermark) {
        // No-Op. We don't use the cache eviction watermark in this implementation
    }

    @Override
    public double getCacheEvictionWatermark() {
        return config.getCacheEvictionWatermark();
    }

    boolean insert(EntryImpl entry) {
        int entrySize = entry.getLength();

        if (entrySize > segmentSize) {
            log.debug("entrySize {} > segmentSize {}, skip update read cache!", entrySize, segmentSize);
            return false;
        }

        long stamp = lock.readLock();
        try {
            SharedCacheSegment s = segments.get(currentSegmentIdx);

            if (s.insert(entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer())) {
                return true;
            }
        } finally {
            lock.unlockRead(stamp);
        }

        // We could not insert in segment, we to get the write lock and roll-over to
        // next segment
        stamp = lock.writeLock();

        try {
            SharedCacheSegment segment = segments.get(currentSegmentIdx);

            if (segment.insert(entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer())) {
                return true;
            }

            // Roll to next segment
            currentSegmentIdx = (currentSegmentIdx + 1) % segmentsCount;
            segment = segments.get(currentSegmentIdx);
            segment.clear();
            return segment.insert(entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer());
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    EntryImpl get(long ledgerId, long entryId) {
        long stamp = lock.readLock();

        try {
            // We need to check all the segments, starting from the current one and looking
            // backward to minimize the checks for recently inserted entries
            for (int i = 0; i < segmentsCount; i++) {
                int segmentIdx = (currentSegmentIdx + (segmentsCount - i)) % segmentsCount;

                ByteBuf res = segments.get(segmentIdx).get(ledgerId, entryId);
                if (res != null) {
                    return EntryImpl.create(ledgerId, entryId, res);
                }
            }
        } finally {
            lock.unlockRead(stamp);
        }

        return null;
    }

    long getRange(long ledgerId, long firstEntryId, long lastEntryId, List<Entry> results) {
        long totalSize = 0;
        long stamp = lock.readLock();

        try {
            // We need to check all the segments, starting from the current one and looking
            // backward to minimize the checks for recently inserted entries
            long entryId = firstEntryId;
            for (int i = 0; i < segmentsCount; i++) {
                int segmentIdx = (currentSegmentIdx + (segmentsCount - i)) % segmentsCount;
                SharedCacheSegment s = segments.get(segmentIdx);

                for (; entryId <= lastEntryId; entryId++) {
                    ByteBuf res = s.get(ledgerId, entryId);
                    if (res != null) {
                        results.add(EntryImpl.create(ledgerId, entryId, res));
                        totalSize += res.readableBytes();
                    } else {
                        break;
                    }
                }

                if (entryId == lastEntryId) {
                    break;
                }
            }
        } finally {
            lock.unlockRead(stamp);
        }

        return totalSize;
    }
}
