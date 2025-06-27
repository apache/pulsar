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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;

public final class EntryImpl extends AbstractEntryImpl<EntryImpl> {
    private static final Recycler<EntryImpl> RECYCLER = new Recycler<EntryImpl>() {
        @Override
        protected EntryImpl newObject(Handle<EntryImpl> handle) {
            return new EntryImpl(handle);
        }
    };

    public static EntryImpl create(LedgerEntry ledgerEntry, ManagedLedgerInterceptor interceptor) {
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
        EntryImpl returnEntry = create(ledgerEntry);
        if (processorHandle != null) {
            processorHandle.release();
            ledgerEntry.close();
        }
        return returnEntry;
    }

    public static EntryImpl create(LedgerEntry ledgerEntry) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = ledgerEntry.getLedgerId();
        entry.entryId = ledgerEntry.getEntryId();
        entry.setDataBuffer(ledgerEntry.getEntryBuffer().retain());
        entry.setRefCnt(1);
        return entry;
    }

    @VisibleForTesting
    public static EntryImpl create(long ledgerId, long entryId, byte[] data) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.setDataBuffer(Unpooled.wrappedBuffer(data));
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(long ledgerId, long entryId, ByteBuf data) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.setDataBuffer(data.retain());
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(Position position, ByteBuf data) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = position.getLedgerId();
        entry.entryId = position.getEntryId();
        entry.setDataBuffer(data.retain());
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(Entry other) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = other.getLedgerId();
        entry.entryId = other.getEntryId();
        entry.setDataBuffer(other.getDataBuffer().retainedDuplicate());
        entry.setRefCnt(1);
        return entry;
    }

    private EntryImpl(Recycler.Handle<EntryImpl> recyclerHandle) {
        super(recyclerHandle);
    }
}
