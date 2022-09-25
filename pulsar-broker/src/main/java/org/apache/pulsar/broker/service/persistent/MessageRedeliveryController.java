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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.apache.pulsar.utils.ConcurrentBitmapSortedLongPairSet;

public class MessageRedeliveryController {
    private final ConcurrentBitmapSortedLongPairSet messagesToRedeliver;
    private final ConcurrentLongLongPairHashMap hashesToBeBlocked;

    public MessageRedeliveryController(boolean allowOutOfOrderDelivery) {
        this.messagesToRedeliver = new ConcurrentBitmapSortedLongPairSet();
        this.hashesToBeBlocked = allowOutOfOrderDelivery
                ? null
                : ConcurrentLongLongPairHashMap
                    .newBuilder().concurrencyLevel(2).expectedItems(128).autoShrink(true).build();
    }

    public void add(long ledgerId, long entryId) {
        messagesToRedeliver.add(ledgerId, entryId);
    }

    public void add(long ledgerId, long entryId, long stickyKeyHash) {
        if (hashesToBeBlocked != null) {
            hashesToBeBlocked.put(ledgerId, entryId, stickyKeyHash, 0);
        }
        messagesToRedeliver.add(ledgerId, entryId);
    }

    public void remove(long ledgerId, long entryId) {
        if (hashesToBeBlocked != null) {
            hashesToBeBlocked.remove(ledgerId, entryId);
        }
        messagesToRedeliver.remove(ledgerId, entryId);
    }

    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
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
        messagesToRedeliver.removeUpTo(markDeleteLedgerId, markDeleteEntryId + 1);
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
        return messagesToRedeliver.items(maxMessagesToRead, PositionImpl::new);
    }
}
