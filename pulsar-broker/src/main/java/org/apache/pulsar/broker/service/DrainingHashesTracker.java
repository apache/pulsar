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
    int batchLevel;
    boolean unblockedWhileBatching;

    /**
     * Represents an entry in the draining hashes tracker.
     */
    public static class DrainingHashEntry {
        private final long consumerId;
        private int refCount;
        private int blockedCount;

        /**
         * Constructs a new DrainingHashEntry with the specified consumer ID.
         *
         * @param consumerId the ID of the consumer
         */
        DrainingHashEntry(long consumerId) {
            this.consumerId = consumerId;
        }

        /**
         * Gets the consumer ID.
         *
         * @return the consumer ID
         */
        public long getConsumerId() {
            return consumerId;
        }

        /**
         * Increments the reference count.
         */
        void incrementRefCount() {
            refCount++;
        }

        /**
         * Decrements the reference count.
         *
         * @return true if the reference count is zero, false otherwise
         */
        boolean decrementRefCount() {
            return --refCount == 0;
        }

        /**
         * Increments the blocked count.
         */
        void incrementBlockedCount() {
            blockedCount++;
        }

        /**
         * Checks if the entry is blocking.
         *
         * @return true if the blocked count is greater than zero, false otherwise
         */
        boolean isBlocking() {
            return blockedCount > 0;
        }

        /**
         * Returns a string representation of the DrainingHashEntry.
         *
         * @return a string representation of the DrainingHashEntry
         */
        public String toString() {
            return "DrainingHashEntry(consumerId=" + this.consumerId + ", refCount=" + this.refCount + ", blockedCount="
                    + this.blockedCount + ")";
        }
    }

    /**
     * Interface for handling the unblocking of sticky key hashes.
     */
    public interface UnblockingHandler {
        /**
         * Handle the unblocking of a sticky key hash.
         *
         * @param stickyKeyHash the sticky key hash that has been unblocked, or -1 if hash unblocking is done in batch
         */
        void stickyKeyHashUnblocked(int stickyKeyHash);
    }

    public DrainingHashesTracker(String dispatcherName, UnblockingHandler unblockingHandler) {
        this.dispatcherName = dispatcherName;
        this.unblockingHandler = unblockingHandler;
    }

    /**
     * Add an entry to the draining hashes tracker.
     *
     * @param consumer the consumer
     * @param stickyHash the sticky hash
     */
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

    /**
     * Start a batch operation. There could be multiple nested batch operations.
     * The unblocking of sticky key hashes will be done only when the last batch operation ends.
     */
    public synchronized void startBatch() {
        batchLevel++;
    }

    /**
     * End a batch operation.
     */
    public synchronized void endBatch() {
        if (--batchLevel == 0 && unblockedWhileBatching) {
            unblockedWhileBatching = false;
            unblockingHandler.stickyKeyHashUnblocked(-1);
        }
    }

    /**
     * Reduce the reference count for a given sticky hash.
     *
     * @param consumer   the consumer
     * @param stickyHash the sticky hash
     * @param closing
     */
    public synchronized void reduceRefCount(Consumer consumer, int stickyHash, boolean closing) {
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
            if (!closing && removed.isBlocking()) {
                if (batchLevel > 0) {
                    unblockedWhileBatching = true;
                } else {
                    unblockingHandler.stickyKeyHashUnblocked(stickyHash);
                }
            }
        }
    }

    /**
     * Check if a sticky key hash should be blocked.
     *
     * @param consumer the consumer
     * @param stickyKeyHash the sticky key hash
     * @return true if the sticky key hash should be blocked, false otherwise
     */
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

    /**
     * Get the entry for a given sticky key hash.
     *
     * @param stickyKeyHash the sticky key hash
     * @return the draining hash entry, or null if not found
     */
    public synchronized DrainingHashEntry getEntry(int stickyKeyHash) {
        return stickyKeyHash != 0 ? drainingHashes.get(stickyKeyHash) : null;
    }

    /**
     * Clear all entries in the draining hashes tracker.
     */
    public synchronized void clear() {
        drainingHashes.clear();
    }
}