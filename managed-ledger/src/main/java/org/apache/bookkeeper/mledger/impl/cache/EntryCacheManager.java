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
package org.apache.bookkeeper.mledger.impl.cache;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;

public interface EntryCacheManager extends AutoCloseable {
    EntryCache getEntryCache(ManagedLedgerImpl ml);

    void removeEntryCache(String name);

    long getSize();

    long getMaxSize();

    void clear();

    void updateCacheSizeAndThreshold(long maxSize);

    void updateCacheEvictionWatermark(double cacheEvictionWatermark);

    double getCacheEvictionWatermark();

    static Entry create(long ledgerId, long entryId, ByteBuf data) {
        return EntryImpl.create(ledgerId, entryId, data);
    }

    static EntryImpl create(LedgerEntry ledgerEntry, ManagedLedgerInterceptor interceptor) {
        ManagedLedgerInterceptor.PayloadProcessorHandle processorHandle = null;
        if (interceptor != null) {
            ByteBuf duplicateBuffer = ledgerEntry.getEntryBuffer().retainedDuplicate();
            processorHandle = interceptor
                    .processPayloadBeforeEntryCache(duplicateBuffer);
            if (processorHandle != null) {
                ledgerEntry  = LedgerEntryImpl.create(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId(),
                        ledgerEntry.getLength(), processorHandle.getProcessedPayload());
            } else {
                duplicateBuffer.release();
            }
        }
        EntryImpl returnEntry = EntryImpl.create(ledgerEntry);
        if (processorHandle != null) {
            processorHandle.release();
            ledgerEntry.close();
        }
        return returnEntry;
    }

    @Override
    void close();
}
