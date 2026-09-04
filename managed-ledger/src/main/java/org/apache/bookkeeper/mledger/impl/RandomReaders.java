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
package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.atomic.AtomicInteger;

public final class RandomReaders {
    private final ManagedLedgerImpl ledger;
    private final AtomicInteger activeReaders = new AtomicInteger();
    private final AtomicInteger cachePopulatingReaders = new AtomicInteger();
    private volatile boolean closed;

    RandomReaders(ManagedLedgerImpl ledger) {
        this.ledger = ledger;
    }

    public synchronized RandomReader create(boolean populateCache) {
        if (closed) {
            throw new IllegalStateException("Managed ledger is already closed");
        }
        RandomReader reader = new RandomReader(ledger, this, populateCache);
        activeReaders.incrementAndGet();
        if (populateCache) {
            cachePopulatingReaders.incrementAndGet();
        }
        return reader;
    }

    synchronized void unregister(boolean populateCache) {
        // Guard against negative: closeAll() may have zeroed counters before a late unregister.
        activeReaders.updateAndGet(current -> Math.max(0, current - 1));
        if (populateCache) {
            cachePopulatingReaders.updateAndGet(current -> Math.max(0, current - 1));
        }
    }

    boolean hasCachePopulatingReaders() {
        return cachePopulatingReaders.get() > 0;
    }

    int cachePopulatingCount() {
        return cachePopulatingReaders.get();
    }

    int size() {
        return activeReaders.get();
    }

    boolean isClosed() {
        return closed;
    }

    synchronized void closeAll() {
        closed = true;
        activeReaders.set(0);
        cachePopulatingReaders.set(0);
    }
}
