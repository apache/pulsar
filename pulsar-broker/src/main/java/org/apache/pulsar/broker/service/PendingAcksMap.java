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

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap;

/**
 * A thread-safe map to store pending acks in the consumer.
 *
 * The locking solution is used for the draining hashes solution
 * to ensure that there's a consistent view of the pending acks. This is needed in the DrainingHashesTracker
 * to ensure that the reference counts are consistent at all times.
 * Calling forEachAndClose will ensure that no more entries can be added,
 * therefore no other thread cannot send out entries while the forEachAndClose is being called.
 * remove is also locked to ensure that there aren't races in the removal of entries while forEachAndClose is
 * running.
 */
public class PendingAcksMap {
    public interface PendingAcksAddHandler {
        boolean handleAdding(long consumerId, long ledgerId, long entryId, int stickyKeyHash);
    }

    private final ConcurrentLongLongPairHashMap pendingAcks;
    private final Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean closed = false;

    PendingAcksMap(ConcurrentLongLongPairHashMap pendingAcks,
                   Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier) {
        this.pendingAcks = pendingAcks;
        this.pendingAcksAddHandlerSupplier = pendingAcksAddHandlerSupplier;
    }

    public boolean put(long consumerId, long ledgerId, long entryId, int batchSize, int stickyKeyHash) {
        try {
            lock.lock();
            if (closed) {
                return false;
            }
            // prevent adding sticky hash to pending acks if it's already in draining hashes
            // to avoid any race conditions that would break consistency
            PendingAcksAddHandler pendingAcksAddHandler = pendingAcksAddHandlerSupplier.get();
            if (pendingAcksAddHandler != null
                    && !pendingAcksAddHandler.handleAdding(consumerId, ledgerId, entryId, stickyKeyHash)) {
                return false;
            }
            pendingAcks.put(ledgerId, entryId, batchSize, stickyKeyHash);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public long size() {
        return pendingAcks.size();
    }

    public void forEach(ConcurrentLongLongPairHashMap.BiConsumerLongPair processor) {
        pendingAcks.forEach(processor);
    }

    public void forEachAndLock(ConcurrentLongLongPairHashMap.BiConsumerLongPair processor) {
        try {
            lock.lock();
            pendingAcks.forEach(processor);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Iterate over all the pending acks and close the map so that no more entries can be added.
     * @param processor
     */
    public void forEachAndClose(ConcurrentLongLongPairHashMap.BiConsumerLongPair processor) {
        try {
            lock.lock();
            closed = true;
            pendingAcks.forEach(processor);
            pendingAcks.clear();
        } finally {
            lock.unlock();
        }
    }

    public boolean contains(long ledgerId, long entryId) {
        return pendingAcks.get(ledgerId, entryId) != null;
    }

    public ConcurrentLongLongPairHashMap.LongPair get(long ledgerId, long entryId) {
        return pendingAcks.get(ledgerId, entryId);
    }

    public boolean remove(long ledgerId, long entryId, long batchSize, long stickyKeyHash) {
        try {
            lock.lock();
            return pendingAcks.remove(ledgerId, entryId, batchSize, stickyKeyHash);
        } finally {
            lock.unlock();
        }
    }

    public boolean remove(long ledgerId, long entryId) {
        try {
            lock.lock();
            return pendingAcks.remove(ledgerId, entryId);
        } finally {
            lock.unlock();
        }
    }
}
