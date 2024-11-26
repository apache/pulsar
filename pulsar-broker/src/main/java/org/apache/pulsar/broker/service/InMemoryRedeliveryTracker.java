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
package org.apache.pulsar.broker.service;

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import org.apache.bookkeeper.mledger.Position;

public class InMemoryRedeliveryTracker extends StampedLock implements RedeliveryTracker {
    // ledgerId -> entryId -> count
    private Long2ObjectMap<Long2IntMap> trackerCache = new Long2ObjectOpenHashMap<>();

    @Override
    public int incrementAndGetRedeliveryCount(Position position) {
        long stamp = writeLock();
        int newCount;
        try {
            Long2IntMap entryMap = trackerCache.computeIfAbsent(position.getLedgerId(),
                    k -> new Long2IntOpenHashMap());
            newCount = entryMap.getOrDefault(position.getEntryId(), 0) + 1;
            entryMap.put(position.getEntryId(), newCount);
        } finally {
            unlockWrite(stamp);
        }
        return newCount;
    }

    @Override
    public int getRedeliveryCount(long ledgerId, long entryId) {
        long stamp = tryOptimisticRead();
        Long2IntMap entryMap = trackerCache.get(ledgerId);
        int count = entryMap != null ? entryMap.get(entryId) : 0;
        if (!validate(stamp)) {
            stamp = readLock();
            try {
                entryMap = trackerCache.get(ledgerId);
                count = entryMap != null ? entryMap.get(entryId) : 0;
            } finally {
                unlockRead(stamp);
            }
        }
        return count;
    }

    @Override
    public void remove(Position position) {
        long stamp = writeLock();
        try {
            Long2IntMap entryMap = trackerCache.get(position.getLedgerId());
            if (entryMap != null) {
                entryMap.remove(position.getEntryId());
                if (entryMap.isEmpty()) {
                    trackerCache.remove(position.getLedgerId());
                }
            }
        } finally {
            unlockWrite(stamp);
        }
    }

    @Override
    public void removeBatch(List<Position> positions) {
        if (positions == null) {
            return;
        }
        long stamp = writeLock();
        try {
            for (Position position : positions) {
                Long2IntMap entryMap = trackerCache.get(position.getLedgerId());
                if (entryMap != null) {
                    entryMap.remove(position.getEntryId());
                    if (entryMap.isEmpty()) {
                        trackerCache.remove(position.getLedgerId());
                    }
                }
            }
        } finally {
            unlockWrite(stamp);
        }
    }

    @Override
    public void clear() {
        long stamp = writeLock();
        try {
            trackerCache.clear();
        } finally {
            unlockWrite(stamp);
        }
    }
}
