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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.StickyKeyConsumerSelector.STICKY_KEY_HASH_NOT_SET;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.apache.pulsar.utils.ConcurrentBitmapSortedLongPairSet;

/**
 * The MessageRedeliveryController is a non-thread-safe container for maintaining the redelivery messages.
 */
@NotThreadSafe
public class MessageRedeliveryController {

    private final boolean allowOutOfOrderDelivery;
    private final boolean isClassicDispatcher;
    private final ConcurrentBitmapSortedLongPairSet messagesToRedeliver;
    private final ConcurrentLongLongPairHashMap hashesToBeBlocked;
    private final ConcurrentLongLongHashMap hashesRefCount;

    public MessageRedeliveryController(boolean allowOutOfOrderDelivery) {
        this(allowOutOfOrderDelivery, false);
    }

    public MessageRedeliveryController(boolean allowOutOfOrderDelivery, boolean isClassicDispatcher) {
        this.allowOutOfOrderDelivery = allowOutOfOrderDelivery;
        this.isClassicDispatcher = isClassicDispatcher;
        this.messagesToRedeliver = new ConcurrentBitmapSortedLongPairSet();
        if (!allowOutOfOrderDelivery) {
            this.hashesToBeBlocked = ConcurrentLongLongPairHashMap
                    .newBuilder().concurrencyLevel(2).expectedItems(128).autoShrink(true).build();
            this.hashesRefCount = ConcurrentLongLongHashMap
                    .newBuilder().concurrencyLevel(2).expectedItems(128).autoShrink(true).build();
        } else {
            this.hashesToBeBlocked = null;
            this.hashesRefCount = null;
        }
    }

    public void add(long ledgerId, long entryId) {
        messagesToRedeliver.add(ledgerId, entryId);
    }

    public void add(long ledgerId, long entryId, long stickyKeyHash) {
        if (!allowOutOfOrderDelivery) {
            if (!isClassicDispatcher && stickyKeyHash == STICKY_KEY_HASH_NOT_SET) {
                throw new IllegalArgumentException("Sticky key hash is not set. It is required.");
            }
            boolean inserted = hashesToBeBlocked.putIfAbsent(ledgerId, entryId, stickyKeyHash, 0);
            if (!inserted) {
                hashesToBeBlocked.put(ledgerId, entryId, stickyKeyHash, 0);
            } else {
                // Return -1 means the key was not present
                long stored = hashesRefCount.get(stickyKeyHash);
                hashesRefCount.put(stickyKeyHash, stored > 0 ? ++stored : 1);
            }
        }
        messagesToRedeliver.add(ledgerId, entryId);
    }

    public void remove(long ledgerId, long entryId) {
        if (!allowOutOfOrderDelivery) {
            removeFromHashBlocker(ledgerId, entryId);
        }
        messagesToRedeliver.remove(ledgerId, entryId);
    }

    private void removeFromHashBlocker(long ledgerId, long entryId) {
        LongPair value = hashesToBeBlocked.get(ledgerId, entryId);
        if (value != null) {
            boolean removed = hashesToBeBlocked.remove(ledgerId, entryId, value.first, 0);
            if (removed) {
                long exists = hashesRefCount.get(value.first);
                if (exists == 1) {
                    hashesRefCount.remove(value.first, exists);
                } else if (exists > 0) {
                    hashesRefCount.put(value.first, exists - 1);
                }
            }
        }
    }

    public Long getHash(long ledgerId, long entryId) {
        LongPair value = hashesToBeBlocked.get(ledgerId, entryId);
        if (value == null) {
            return null;
        }
        return value.first;
    }

    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
        boolean bitsCleared = messagesToRedeliver.removeUpTo(markDeleteLedgerId, markDeleteEntryId + 1);
        // only if bits have been clear, and we are not allowing out of order delivery, we need to remove the hashes
        // removing hashes is a relatively expensive operation, so we should only do it when necessary
        if (bitsCleared && !allowOutOfOrderDelivery) {
            List<LongPair> keysToRemove = new ArrayList<>();
            hashesToBeBlocked.forEach((ledgerId, entryId, stickyKeyHash, none) -> {
                if (ledgerId < markDeleteLedgerId || (ledgerId == markDeleteLedgerId && entryId <= markDeleteEntryId)) {
                    keysToRemove.add(new LongPair(ledgerId, entryId));
                }
            });
            for (LongPair longPair : keysToRemove) {
                removeFromHashBlocker(longPair.first, longPair.second);
            }
        }
    }

    public boolean isEmpty() {
        return messagesToRedeliver.isEmpty();
    }

    public void clear() {
        if (!allowOutOfOrderDelivery) {
            hashesToBeBlocked.clear();
            hashesRefCount.clear();
        }
        messagesToRedeliver.clear();
    }

    public String toString() {
        return messagesToRedeliver.toString();
    }

    public boolean containsStickyKeyHashes(Set<Integer> stickyKeyHashes) {
        if (!allowOutOfOrderDelivery) {
            for (Integer stickyKeyHash : stickyKeyHashes) {
                if (stickyKeyHash != STICKY_KEY_HASH_NOT_SET && hashesRefCount.containsKey(stickyKeyHash)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean containsStickyKeyHash(int stickyKeyHash) {
        return !allowOutOfOrderDelivery
                && stickyKeyHash != STICKY_KEY_HASH_NOT_SET && hashesRefCount.containsKey(stickyKeyHash);
    }

    public Optional<Position> getFirstPositionInReplay() {
        return messagesToRedeliver.first(PositionFactory::create);
    }

    /**
     * Get the messages to replay now.
     *
     * @param maxMessagesToRead
     *            the max messages to read
     * @param filter
     *            the filter to use to select the messages to replay
     * @return the messages to replay now
     */
    public NavigableSet<Position> getMessagesToReplayNow(int maxMessagesToRead, Predicate<Position> filter) {
        NavigableSet<Position> items = new TreeSet<>();
        messagesToRedeliver.processItems(PositionFactory::create, item -> {
            if (filter.test(item)) {
                items.add(item);
            }
            return items.size() < maxMessagesToRead;
        });
        return items;
    }

    /**
     * Get the number of messages registered for replay in the redelivery controller.
     *
     * @return number of messages
     */
    public int size() {
        return messagesToRedeliver.size();
    }
}
