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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.apache.pulsar.common.util.collections.ConcurrentSortedLongPairSet;
import org.apache.pulsar.common.util.collections.LongPairSet;

public class MessageRedeliveryController {
    private final LongPairSet messagesToRedeliver;
    private final ConcurrentLongLongPairHashMap hashesToBeBlocked;

    public MessageRedeliveryController(boolean allowOutOfOrderDelivery) {
        this.messagesToRedeliver = new ConcurrentSortedLongPairSet(128, 2);
        this.hashesToBeBlocked = allowOutOfOrderDelivery ? null : new ConcurrentLongLongPairHashMap(128, 2);
    }

    public boolean add(long ledgerId, long entryId) {
        return messagesToRedeliver.add(ledgerId, entryId);
    }

    public boolean add(long ledgerId, long entryId, long stickyKeyHash) {
        if (hashesToBeBlocked != null) {
            hashesToBeBlocked.put(ledgerId, entryId, stickyKeyHash, 0);
        }
        return messagesToRedeliver.add(ledgerId, entryId);
    }

    public boolean remove(long ledgerId, long entryId) {
        if (hashesToBeBlocked != null) {
            hashesToBeBlocked.remove(ledgerId, entryId);
        }
        return messagesToRedeliver.remove(ledgerId, entryId);
    }

    public int removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
        if (hashesToBeBlocked != null) {
            List<LongPair> keysToRemove = new ArrayList<>();
            hashesToBeBlocked.forEach((ledgerId, entryId, stickyKeyHash, none) -> {
                if (ComparisonChain.start().compare(ledgerId, markDeleteLedgerId).compare(entryId, markDeleteEntryId)
                        .result() <= 0) {
                    keysToRemove.add(new LongPair(ledgerId, entryId));
                }
            });
            keysToRemove.forEach(longPair -> hashesToBeBlocked.remove(longPair.first, longPair.second));
            keysToRemove.clear();
        }
        return messagesToRedeliver.removeIf((ledgerId, entryId) -> {
            return ComparisonChain.start().compare(ledgerId, markDeleteLedgerId).compare(entryId, markDeleteEntryId)
                    .result() <= 0;
        });
    }

    public boolean isEmpty() {
        return messagesToRedeliver.isEmpty();
    }

    public void clear() {
        if (hashesToBeBlocked != null) {
            hashesToBeBlocked.clear();
        }
        messagesToRedeliver.clear();
    }

    public String toString() {
        return messagesToRedeliver.toString();
    }

    public boolean containsStickyKeyHashes(Set<Integer> stickyKeyHashes) {
        final AtomicBoolean isContained = new AtomicBoolean(false);
        if (hashesToBeBlocked != null) {
            hashesToBeBlocked.forEach((ledgerId, entryId, stickyKeyHash, none) -> {
                if (!isContained.get() && stickyKeyHashes.contains((int) stickyKeyHash)) {
                    isContained.set(true);
                }
            });
        }
        return isContained.get();
    }

    public Set<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        if (hashesToBeBlocked != null) {
            // allowOutOfOrderDelivery is false
            return messagesToRedeliver.items().stream()
                    .sorted((l1, l2) -> ComparisonChain.start().compare(l1.first, l2.first)
                            .compare(l1.second, l2.second).result())
                    .limit(maxMessagesToRead).map(longPair -> new PositionImpl(longPair.first, longPair.second))
                    .collect(Collectors.toCollection(TreeSet::new));
        } else {
            // allowOutOfOrderDelivery is true
            return messagesToRedeliver.items(maxMessagesToRead,
                    (ledgerId, entryId) -> new PositionImpl(ledgerId, entryId));
        }
    }
}
