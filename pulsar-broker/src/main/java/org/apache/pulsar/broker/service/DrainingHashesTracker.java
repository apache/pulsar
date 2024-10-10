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
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.DrainingHash;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.DrainingHashImpl;
import org.roaringbitmap.RoaringBitmap;

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
    private final Map<ConsumerIdentityWrapper, ConsumerDrainingHashesStats> consumerDrainingHashesStatsMap =
            new ConcurrentHashMap<>();

    /**
     * Represents an entry in the draining hashes tracker.
     */
    @ToString
    public static class DrainingHashEntry {
        private final Consumer consumer;
        private int refCount;
        private int blockedCount;

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
    }

    private class ConsumerDrainingHashesStats {
        private final RoaringBitmap drainingHashes = new RoaringBitmap();
        long drainingHashesClearedTotal;

        public synchronized void addHash(int stickyHash) {
            drainingHashes.add(stickyHash);
        }

        public synchronized boolean clearHash(int hash) {
            drainingHashes.remove(hash);
            drainingHashesClearedTotal++;
            boolean empty = drainingHashes.isEmpty();
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cleared hash {} in stats. empty={} totalCleared={} hashes={}",
                        dispatcherName, hash, empty, drainingHashesClearedTotal, drainingHashes.getCardinality());
            }
            return empty;
        }

        public synchronized void updateConsumerStats(Consumer consumer, ConsumerStatsImpl consumerStats) {
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
                int unackedMessages = entry.refCount;
                DrainingHashImpl drainingHash = new DrainingHashImpl();
                drainingHash.hash = hash;
                drainingHash.unackMsgs = unackedMessages;
                drainingHash.blockedAttempts = entry.blockedCount;
                drainingHashesStats.add(drainingHash);
                drainingHashesUnackedMessages += unackedMessages;
            }
            consumerStats.drainingHashesCount = drainingHashesStats.size();
            consumerStats.drainingHashesClearedTotal = drainingHashesClearedTotal;
            consumerStats.drainingHashesUnackedMessages = drainingHashesUnackedMessages;
            consumerStats.drainingHashes = drainingHashesStats;
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
            if (log.isDebugEnabled()) {
                log.debug("[{}] Adding and incrementing draining hash {} for consumer id:{} name:{}", dispatcherName,
                        stickyHash, consumer.consumerId(), consumer.consumerName());
            }
            entry = new DrainingHashEntry(consumer);
            drainingHashes.put(stickyHash, entry);
            // update the consumer specific stats
            consumerDrainingHashesStatsMap.computeIfAbsent(new ConsumerIdentityWrapper(consumer),
                    k -> new ConsumerDrainingHashesStats()).addHash(stickyHash);
        } else if (entry.getConsumer() != consumer) {
            throw new IllegalStateException(
                    "Consumer " + entry.getConsumer() + " is already draining hash " + stickyHash
                            + " in dispatcher " + dispatcherName + ". Same hash being used for consumer " + consumer
                            + ".");
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Draining hash {} incrementing {} consumer id:{} name:{}", dispatcherName, stickyHash,
                        entry.refCount + 1, consumer.consumerId(), consumer.consumerName());
            }
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
            DrainingHashEntry removed = drainingHashes.remove(stickyHash);
            // update the consumer specific stats
            ConsumerDrainingHashesStats drainingHashesStats =
                    consumerDrainingHashesStatsMap.get(new ConsumerIdentityWrapper(consumer));
            if (drainingHashesStats != null) {
                drainingHashesStats.clearHash(stickyHash);
            }
            if (!closing && removed.isBlocking()) {
                if (batchLevel > 0) {
                    unblockedWhileBatching = true;
                } else {
                    unblockingHandler.stickyKeyHashUnblocked(stickyHash);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Draining hash {} decrementing {} consumer id:{} name:{}", dispatcherName, stickyHash,
                        entry.refCount, consumer.consumerId(), consumer.consumerName());
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
        if (stickyKeyHash == STICKY_KEY_HASH_NOT_SET) {
            log.warn("[{}] Sticky key hash is not set. Allowing dispatching", dispatcherName);
            return false;
        }
        DrainingHashEntry entry = drainingHashes.get(stickyKeyHash);
        // if the entry is not found, the hash is not draining. Don't block the hash.
        if (entry == null) {
            return false;
        }
        // hash has been reassigned to the original consumer, remove the entry
        // and don't block the hash
        if (entry.getConsumer() == consumer) {
            log.info("[{}] Hash {} has been reassigned consumer {}. "
                            + "The draining hash entry with refCount={} will be removed.",
                    dispatcherName, stickyKeyHash, entry.getConsumer(), entry.refCount);
            drainingHashes.remove(stickyKeyHash, entry);
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
    public synchronized DrainingHashEntry getEntry(int stickyKeyHash) {
        return stickyKeyHash != 0 ? drainingHashes.get(stickyKeyHash) : null;
    }

    /**
     * Clear all entries in the draining hashes tracker.
     */
    public synchronized void clear() {
        drainingHashes.clear();
        consumerDrainingHashesStatsMap.clear();
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