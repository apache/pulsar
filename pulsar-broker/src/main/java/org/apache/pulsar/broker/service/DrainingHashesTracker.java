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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * A thread-safe map to store draining hashes in the consumer.
 */
@Slf4j
public class DrainingHashesTracker {
    private final String dispatcherName;
    private final UnblockingHandler unblockingHandler;
    // optimize the memory consumption of the map by using primitive int keys
    private final Int2ObjectOpenHashMap<DrainingHashEntry> drainingHashes = new Int2ObjectOpenHashMap<>();

    public static class DrainingHashEntry {
        private final long consumerId;
        private int refCount;
        private int blockedCount;

        DrainingHashEntry(long consumerId) {
            this.consumerId = consumerId;
        }

        public long getConsumerId() {
            return consumerId;
        }

        void incrementRefCount() {
            refCount++;
        }

        boolean decrementRefCount() {
            return --refCount == 0;
        }

        void incrementBlockedCount() {
            blockedCount++;
        }

        boolean isBlocking() {
            return blockedCount > 0;
        }

        public String toString() {
            return "DrainingHashEntry(consumerId=" + this.consumerId + ", refCount=" + this.refCount + ", blockedCount="
                    + this.blockedCount + ")";
        }
    }

    public interface UnblockingHandler {
        void stickyKeyHashUnblocked(int stickyKeyHash);
    }

    public DrainingHashesTracker(String dispatcherName, UnblockingHandler unblockingHandler) {
        this.dispatcherName = dispatcherName;
        this.unblockingHandler = unblockingHandler;
    }

    public synchronized void addEntry(Consumer consumer, int stickyHash) {
        if (stickyHash == 0) {
            throw new IllegalArgumentException("Sticky hash cannot be 0");
        }
        DrainingHashEntry entry = drainingHashes.get(stickyHash);
        if (entry == null) {
            entry = new DrainingHashEntry(consumer.consumerId());
            drainingHashes.put(stickyHash, entry);
        }
        if (entry.getConsumerId() != consumer.consumerId()) {
            log.error("[{}] Consumer id {} is already draining hash {}. Same hash being used for consumer {}. "
                            + "This call is ignored since there's a problem in consistency.",
                    dispatcherName,
                    entry.getConsumerId(), stickyHash, consumer.consumerId());
            return;
        }
        entry.incrementRefCount();
    }

    public synchronized void reduceRefCount(Consumer consumer, int stickyHash) {
        if (stickyHash == 0) {
            return;
        }
        DrainingHashEntry entry = drainingHashes.get(stickyHash);
        if (entry == null) {
            return;
        }
        if (entry.getConsumerId() != consumer.consumerId()) {
            log.error("[{}] Consumer id {} is already draining hash {}. Same hash is being acknowledged by consumer {}."
                            + " This call is ignored since there's a problem in consistency.",
                    dispatcherName,
                    entry.getConsumerId(), stickyHash, consumer.consumerId());
            return;
        }
        if (entry.decrementRefCount()) {
            DrainingHashEntry removed = drainingHashes.remove(stickyHash);
            if (removed.isBlocking()) {
                unblockingHandler.stickyKeyHashUnblocked(stickyHash);
            }
        }
    }

    public synchronized boolean shouldBlockStickyKeyHash(Consumer consumer, int stickyKeyHash) {
        if (stickyKeyHash == 0) {
            log.warn("[{}] Sticky key hash is 0. Allowing dispatching", dispatcherName);
            return false;
        }
        DrainingHashEntry entry = drainingHashes.get(stickyKeyHash);
        if (entry == null) {
            return false;
        }
        // hash has been reassigned to the original consumer, remove the entry
        if (entry.getConsumerId() == consumer.consumerId()) {
            log.info("[{}] Hash {} has been reassigned consumer {}. "
                            + "The draining hash entry with refCount={} will be removed.",
                    dispatcherName, stickyKeyHash, entry.getConsumerId(), entry.refCount);
            drainingHashes.remove(stickyKeyHash, entry);
            return false;
        }
        entry.incrementBlockedCount();
        return true;
    }

    public synchronized DrainingHashEntry getEntry(int stickyKeyHash) {
        return stickyKeyHash != 0 ? drainingHashes.get(stickyKeyHash) : null;
    }

    public synchronized void clear() {
        drainingHashes.clear();
    }
}
