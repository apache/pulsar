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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.testng.annotations.Test;

public class RangeEntryCacheImplTest {
    @Test
    public void testReadFromStorageRetriesWhenHandleClosed() {
        ManagedLedgerFactoryImpl mockFactory = mock(ManagedLedgerFactoryImpl.class);
        ManagedLedgerFactoryMBeanImpl mockFactoryMBean = mock(ManagedLedgerFactoryMBeanImpl.class);
        when(mockFactory.getMbean()).thenReturn(mockFactoryMBean);
        when(mockFactory.getConfig()).thenReturn(new ManagedLedgerFactoryConfig());
        RangeEntryCacheManagerImpl mockEntryCacheManager = spy(new RangeEntryCacheManagerImpl(mockFactory, mock(
                OrderedScheduler.class), OpenTelemetry.noop()));
        ManagedLedgerImpl mockManagedLedger = mock(ManagedLedgerImpl.class);
        ManagedLedgerMBeanImpl mockManagedLedgerMBean = mock(ManagedLedgerMBeanImpl.class);
        when(mockManagedLedger.getMbean()).thenReturn(mockManagedLedgerMBean);
        when(mockManagedLedger.getName()).thenReturn("testManagedLedger");
        when(mockManagedLedger.getExecutor()).thenReturn(mock(java.util.concurrent.ExecutorService.class));
        when(mockManagedLedger.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        InflightReadsLimiter inflightReadsLimiter = mock(InflightReadsLimiter.class);
        when(mockEntryCacheManager.getInflightReadsLimiter()).thenReturn(inflightReadsLimiter);
        doAnswer(invocation -> {
            long permits = invocation.getArgument(0);
            InflightReadsLimiter.Handle handle = new InflightReadsLimiter.Handle(permits, System.currentTimeMillis(),
                    true);
            return Optional.of(handle);
        }).when(inflightReadsLimiter).acquire(anyLong(), any());

        RangeEntryCacheImpl cache = new RangeEntryCacheImpl(mockEntryCacheManager, mockManagedLedger, false);

        ReadHandle readHandle = mock(ReadHandle.class);
        when(readHandle.getId()).thenReturn(1L);
        when(mockManagedLedger.reopenReadHandle(1L)).thenReturn(CompletableFuture.completedFuture(readHandle));

        LedgerEntryImpl ledgerEntry = LedgerEntryImpl.create(1L, 0L, 1, Unpooled.wrappedBuffer(new byte[]{1}));
        LedgerEntries ledgerEntries = mock(LedgerEntries.class);
        List<LedgerEntry> entryList = List.of((LedgerEntry) ledgerEntry);
        when(ledgerEntries.iterator()).thenReturn(entryList.iterator());

        AtomicInteger readAttempts = new AtomicInteger();
        when(readHandle.readAsync(0L, 0L)).thenAnswer(invocation -> {
            if (readAttempts.getAndIncrement() == 0) {
                return CompletableFuture.failedFuture(new ManagedLedgerException.OffloadReadHandleClosedException());
            }
            return CompletableFuture.completedFuture(ledgerEntries);
        });

        CompletableFuture<List<EntryImpl>> future = cache.readFromStorage(readHandle, 0L, 0L, true);
        assertThat(future).isCompleted().satisfies(f -> {
            List<EntryImpl> entries = f.getNow(null);
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).getLedgerId()).isEqualTo(1L);
            assertThat(entries.get(0).getEntryId()).isEqualTo(0L);
        });
        assertThat(readAttempts.get()).isEqualTo(2);
    }
}