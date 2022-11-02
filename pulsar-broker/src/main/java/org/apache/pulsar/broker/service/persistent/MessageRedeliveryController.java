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
package org.apache.pulsar.broker.service.persistent;

import com.google.common.collect.ComparisonChain;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
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
    private final ConcurrentBitmapSortedLongPairSet messagesToRedeliver;
    private final ConcurrentLongLongPairHashMap hashesToBeBlocked;
    private final ConcurrentLongLongHashMap hashesRefCount;

    public MessageRedeliveryController(boolean allowOutOfOrderDelivery) {
        this.allowOutOfOrderDelivery = allowOutOfOrderDelivery;
        this.messagesToRedeliver = new ConcurrentBitmapSortedLongPairSet();
        if (!allowOutOfOrderDelivery) {
            this.hashesToBeBlocked = ConcurrentLongLongPairHashMap
                    .newBuilder().concurrencyLevel(2).expectedItems(128).autoShrink(true).build();
            this.hashesRefCount = new ConcurrentLongLongHashMap(128, 2);
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

    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
        if (!allowOutOfOrderDelivery) {
            List<LongPair> keysToRemove = new ArrayList<>();
            hashesToBeBlocked.forEach((ledgerId, entryId, stickyKeyHash, none) -> {
                if (ComparisonChain.start().compare(ledgerId, markDeleteLedgerId).compare(entryId, markDeleteEntryId)
                        .result() <= 0) {
                    keysToRemove.add(new LongPair(ledgerId, entryId));
                }
            });
            keysToRemove.forEach(longPair -> removeFromHashBlocker(longPair.first, longPair.second));
            keysToRemove.clear();
        }
        messagesToRedeliver.removeUpTo(markDeleteLedgerId, markDeleteEntryId + 1);
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
                if (hashesRefCount.containsKey(stickyKeyHash)) {
                    return true;
                }
            }
        }
        return false;
    }

    public NavigableSet<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        return messagesToRedeliver.items(maxMessagesToRead, PositionImpl::new);
    }
}
