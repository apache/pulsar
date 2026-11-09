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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ManagedCursorPersistPositionConcurrencyTest extends MockedBookKeeperTestCase {

    // A cursor's close persists its position concurrently with a mark-delete on another thread, and both serialize it
    // with the cursor's reused position info
    @Test(timeOut = 60000)
    public void testConcurrentPersistsSerializeTheirOwnPositions() throws Exception {
        ManagedLedger ledger = factory.open("persist_position_concurrency", new ManagedLedgerConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("c1");
        LedgerHandle lh = mock(LedgerHandle.class);
        ConcurrentLinkedQueue<byte[]> written = new ConcurrentLinkedQueue<>();
        doAnswer(invocation -> {
            written.add(invocation.getArgument(0));
            return null;
        }).when(lh).asyncAddEntry(any(byte[].class), any(AddCallback.class), any());

        int threads = 2;
        int persistsPerThread = 20_000;
        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                long ledgerId = t + 1;
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < persistsPerThread; i++) {
                        // each thread's entry IDs are its ledger ID times a million plus a counter
                        long entryId = ledgerId * 1_000_000 + i;
                        try {
                            cursor.persistPositionToLedger(lh, cursor.new MarkDeleteEntry(
                                    PositionFactory.create(ledgerId, entryId), Map.of("thread", ledgerId), null,
                                    null), null, false);
                        } catch (Throwable e) {
                            failures.add(e);
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(failures).isEmpty();
        assertThat(written).hasSize(threads * persistsPerThread);
        Set<Long> writtenEntryIds = new HashSet<>();
        for (byte[] data : written) {
            PositionInfo positionInfo = new PositionInfo();
            positionInfo.parseFrom(data);
            // a position info cleared or changed by the other thread while it was built has another thread's
            // ledger ID, or no position, or another thread's properties
            assertThat(positionInfo.getEntryId() / 1_000_000).isEqualTo(positionInfo.getLedgerId());
            assertThat(positionInfo.getPropertiesCount()).isEqualTo(1);
            assertThat(positionInfo.getPropertyAt(0).getValue()).isEqualTo(positionInfo.getLedgerId());
            writtenEntryIds.add(positionInfo.getEntryId());
        }
        // every persist wrote its own position, and none another persist's, which would be consistent too
        Set<Long> submittedEntryIds = new HashSet<>();
        for (long ledgerId = 1; ledgerId <= threads; ledgerId++) {
            for (int i = 0; i < persistsPerThread; i++) {
                submittedEntryIds.add(ledgerId * 1_000_000 + i);
            }
        }
        assertThat(writtenEntryIds).isEqualTo(submittedEntryIds);
    }
}
