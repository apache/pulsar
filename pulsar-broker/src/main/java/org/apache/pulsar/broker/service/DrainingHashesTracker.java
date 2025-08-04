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

import static org.apache.pulsar.broker.service.StickyKeyConsumerSelector.STICKY_KEY_HASH_NOT_SET;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.DrainingHash;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.DrainingHashImpl;
import org.roaringbitmap.RoaringBitmap;

/**
 * A thread-safe map to store draining hashes in the consumer.
 * The implementation uses read-write locks for ensuring thread-safe access. The high-level strategy to prevent
 * deadlocks is to perform side-effects (calls to other collaborators which could have other exclusive locks)
 * outside of the write lock. Early versions of this class had a problem where deadlocks could occur when
 * a consumer operations happened at the same time as another thread requested topic stats which include
 * the draining hashes state. This problem is avoided with the current implementation.
 */
@Slf4j
public class DrainingHashesTracker {
    private final String dispatcherName;
    private final UnblockingHandler unblockingHandler;
    // optimize the memory consumption of the map by using primitive int keys
    private final Int2ObjectOpenHashMap<DrainingHashEntry> drainingHashes = new Int2ObjectOpenHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    int batchLevel;
    boolean unblockedWhileBatching;
    private final Map<ConsumerIdentityWrapper, ConsumerDrainingHashesStats> consumerDrainingHashesStatsMap =
            new ConcurrentHashMap<>();

    /**
     * Represents an entry in the draining hashes tracker.
     */
    @ToString
    public static class DrainingHashEntry {
        private static final AtomicIntegerFieldUpdater<DrainingHashEntry> REF_COUNT_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(DrainingHashEntry.class, "refCount");
        private static final AtomicIntegerFieldUpdater<DrainingHashEntry> BLOCKED_COUNT_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(DrainingHashEntry.class, "blockedCount");

        private final Consumer consumer;
        private volatile int refCount;
        private volatile int blockedCount;

        /**
         * Constructs a new DrainingHashEntry with the specified Consumer.
         *
         * @param consumer the Consumer instance
         */
        DrainingHashEntry(Consumer consumer) {
            this.consumer = consumer;
        }

        /**
         * Gets the consumer that contained the hash in pending acks at the time of creating this
         * entry. Since a particular hash can be assigned to only one consumer at a time, this consumer
         * cannot change. No new pending acks can be added in the {@link PendingAcksMap} when there's
         * a draining hash entry for a hash in {@link DrainingHashesTracker}.
         *
         * @return the consumer instance that contained the hash in pending acks at the time of creating this entry
         */
        public Consumer getConsumer() {
            return consumer;
        }

        /**
         * Increments the reference count.
         */
        void incrementRefCount() {
            REF_COUNT_UPDATER.incrementAndGet(this);
        }

        /**
         * Decrements the reference count.
         *
         * @return true if the reference count is zero, false otherwise
         */
        boolean decrementRefCount() {
            return REF_COUNT_UPDATER.decrementAndGet(this) == 0;
        }

        /**
         * Increments the blocked count.
         */
        void incrementBlockedCount() {
            BLOCKED_COUNT_UPDATER.incrementAndGet(this);
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
         * Gets the current reference count.
         *
         * @return the current reference count
         */
        int getRefCount() {
            return refCount;
        }

        /**
         * Gets the current blocked count.
         *
         * @return the current blocked count
         */
        int getBlockedCount() {
            return blockedCount;
        }
    }

    private class ConsumerDrainingHashesStats {
        private final RoaringBitmap drainingHashes = new RoaringBitmap();
        private long drainingHashesClearedTotal;
        private final ReentrantReadWriteLock statsLock = new ReentrantReadWriteLock();

        public void addHash(int stickyHash) {
            statsLock.writeLock().lock();
            try {
                drainingHashes.add(stickyHash);
            } finally {
                statsLock.writeLock().unlock();
            }
        }

        public boolean clearHash(int hash) {
            statsLock.writeLock().lock();
            try {
                drainingHashes.remove(hash);
                drainingHashesClearedTotal++;
                boolean empty = drainingHashes.isEmpty();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Cleared hash {} in stats. empty={} totalCleared={} hashes={}",
                            dispatcherName, hash, empty, drainingHashesClearedTotal, drainingHashes.getCardinality());
                }
                if (empty) {
                    // reduce memory usage by trimming the bitmap when the RoaringBitmap instance is empty
                    drainingHashes.trim();
                }
                return empty;
            } finally {
                statsLock.writeLock().unlock();
            }
        }

        public void updateConsumerStats(Consumer consumer, ConsumerStatsImpl consumerStats) {
            statsLock.readLock().lock();
            try {
                int drainingHashesUnackedMessages = 0;
                List<DrainingHash> drainingHashesStats = new ArrayList<>();
                PrimitiveIterator.OfInt hashIterator = drainingHashes.stream().iterator();
                while (hashIterator.hasNext()) {
                    int hash = hashIterator.nextInt();
                    DrainingHashEntry entry = getEntry(hash);
                    if (entry == null) {
                        log.warn("[{}] Draining hash {} not found in the tracker for consumer {}", dispatcherName, hash,
                                consumer);
                        continue;
                    }
                    int unackedMessages = entry.getRefCount();
                    DrainingHashImpl drainingHash = new DrainingHashImpl();
                    drainingHash.hash = hash;
                    drainingHash.unackMsgs = unackedMessages;
                    drainingHash.blockedAttempts = entry.getBlockedCount();
                    drainingHashesStats.add(drainingHash);
                    drainingHashesUnackedMessages += unackedMessages;
                }
                consumerStats.drainingHashesCount = drainingHashesStats.size();
                consumerStats.drainingHashesClearedTotal = drainingHashesClearedTotal;
                consumerStats.drainingHashesUnackedMessages = drainingHashesUnackedMessages;
                consumerStats.drainingHashes = drainingHashesStats;
            } finally {
                statsLock.readLock().unlock();
            }
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
    public void addEntry(Consumer consumer, int stickyHash) {
        if (stickyHash == 0) {
            throw new IllegalArgumentException("Sticky hash cannot be 0");
        }

        DrainingHashEntry entry;
        ConsumerDrainingHashesStats addedStatsForNewEntry = null;
        lock.writeLock().lock();
        try {
            entry = drainingHashes.get(stickyHash);
            if (entry == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Adding and incrementing draining hash {} for consumer id:{} name:{}",
                            dispatcherName, stickyHash, consumer.consumerId(), consumer.consumerName());
                }
                entry = new DrainingHashEntry(consumer);
                drainingHashes.put(stickyHash, entry);
                // add the consumer specific stats
                addedStatsForNewEntry = consumerDrainingHashesStatsMap
                        .computeIfAbsent(new ConsumerIdentityWrapper(consumer), k -> new ConsumerDrainingHashesStats());
            } else if (entry.getConsumer() != consumer) {
                throw new IllegalStateException(
                        "Consumer " + entry.getConsumer() + " is already draining hash " + stickyHash
                                + " in dispatcher " + dispatcherName + ". Same hash being used for consumer " + consumer
                                + ".");
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Draining hash {} incrementing {} consumer id:{} name:{}", dispatcherName,
                            stickyHash, entry.getRefCount() + 1, consumer.consumerId(), consumer.consumerName());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        // increment the reference count of the entry (applies to both new and existing entries)
        entry.incrementRefCount();

        // perform side-effects outside of the lock to reduce chances for deadlocks
        if (addedStatsForNewEntry != null) {
            // add hash to added stats
            addedStatsForNewEntry.addHash(stickyHash);
        }
    }

    /**
     * Start a batch operation. There could be multiple nested batch operations.
     * The unblocking of sticky key hashes will be done only when the last batch operation ends.
     */
    public void startBatch() {
        lock.writeLock().lock();
        try {
            batchLevel++;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * End a batch operation.
     */
    public void endBatch() {
        boolean notifyUnblocking = false;
        lock.writeLock().lock();
        try {
            if (--batchLevel == 0 && unblockedWhileBatching) {
                unblockedWhileBatching = false;
                notifyUnblocking = true;
            }
        } finally {
            lock.writeLock().unlock();
        }
        // notify unblocking of the hash outside the lock
        if (notifyUnblocking) {
            unblockingHandler.stickyKeyHashUnblocked(-1);
        }
    }

    /**
     * Reduce the reference count for a given sticky hash.
     *
     * @param consumer   the consumer
     * @param stickyHash the sticky hash
     * @param closing    whether the consumer is closing
     */
    public void reduceRefCount(Consumer consumer, int stickyHash, boolean closing) {
        if (stickyHash == 0) {
            return;
        }

        DrainingHashEntry entry = getEntry(stickyHash);
        if (entry == null) {
            return;
        }
        if (entry.getConsumer() != consumer) {
            throw new IllegalStateException(
                    "Consumer " + entry.getConsumer() + " is already draining hash " + stickyHash
                            + " in dispatcher " + dispatcherName + ". Same hash being used for consumer " + consumer
                            + ".");
        }
        if (entry.decrementRefCount()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Draining hash {} removing consumer id:{} name:{}", dispatcherName, stickyHash,
                        consumer.consumerId(), consumer.consumerName());
            }

            DrainingHashEntry removed;
            boolean notifyUnblocking = false;
            lock.writeLock().lock();
            try {
                removed = drainingHashes.remove(stickyHash);
                if (!closing && removed.isBlocking()) {
                    if (batchLevel > 0) {
                        unblockedWhileBatching = true;
                    } else {
                        notifyUnblocking = true;
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }

            // perform side-effects outside of the lock to reduce chances for deadlocks

            // update the consumer specific stats
            ConsumerDrainingHashesStats drainingHashesStats =
                    consumerDrainingHashesStatsMap.get(new ConsumerIdentityWrapper(consumer));
            if (drainingHashesStats != null) {
                drainingHashesStats.clearHash(stickyHash);
            }

            // notify unblocking of the hash outside the lock
            if (notifyUnblocking) {
                unblockingHandler.stickyKeyHashUnblocked(stickyHash);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Draining hash {} decrementing {} consumer id:{} name:{}", dispatcherName,
                        stickyHash, entry.getRefCount(), consumer.consumerId(), consumer.consumerName());
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
    public boolean shouldBlockStickyKeyHash(Consumer consumer, int stickyKeyHash) {
        if (stickyKeyHash == STICKY_KEY_HASH_NOT_SET) {
            log.warn("[{}] Sticky key hash is not set. Allowing dispatching", dispatcherName);
            return false;
        }
        DrainingHashEntry entry = getEntry(stickyKeyHash);
        // if the entry is not found, the hash is not draining. Don't block the hash.
        if (entry == null) {
            return false;
        }
        // hash has been reassigned to the original consumer, remove the entry
        // and don't block the hash
        if (entry.getConsumer() == consumer) {
            log.info("[{}] Hash {} has been reassigned consumer {}. The draining hash entry with refCount={} will "
                    + "be removed.", dispatcherName, stickyKeyHash, entry.getConsumer(), entry.getRefCount());
            lock.writeLock().lock();
            try {
                drainingHashes.remove(stickyKeyHash, entry);
            } finally {
                lock.writeLock().unlock();
            }
            return false;
        }
        // increment the blocked count which is used to determine if the hash is blocking
        // dispatching to other consumers
        entry.incrementBlockedCount();
        // block the hash
        return true;
    }

    /**
     * Get the entry for a given sticky key hash.
     *
     * @param stickyKeyHash the sticky key hash
     * @return the draining hash entry, or null if not found
     */
    public DrainingHashEntry getEntry(int stickyKeyHash) {
        if (stickyKeyHash == 0) {
            return null;
        }
        lock.readLock().lock();
        try {
            return drainingHashes.get(stickyKeyHash);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clear all entries in the draining hashes tracker.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            drainingHashes.clear();
            consumerDrainingHashesStatsMap.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Update the consumer specific stats to the target {@link ConsumerStatsImpl}.
     *
     * @param consumer the consumer
     * @param consumerStats the consumer stats to update the values to
     */
    public void updateConsumerStats(Consumer consumer, ConsumerStatsImpl consumerStats) {
        consumerStats.drainingHashesCount = 0;
        consumerStats.drainingHashesClearedTotal = 0;
        consumerStats.drainingHashesUnackedMessages = 0;
        consumerStats.drainingHashes = Collections.emptyList();
        ConsumerDrainingHashesStats consumerDrainingHashesStats =
                consumerDrainingHashesStatsMap.get(new ConsumerIdentityWrapper(consumer));
        if (consumerDrainingHashesStats != null) {
            consumerDrainingHashesStats.updateConsumerStats(consumer, consumerStats);
        }
    }

    /**
     * Remove the consumer specific stats from the draining hashes tracker.
     * @param consumer the consumer
     */
    public void consumerRemoved(Consumer consumer) {
        consumerDrainingHashesStatsMap.remove(new ConsumerIdentityWrapper(consumer));
    }
}