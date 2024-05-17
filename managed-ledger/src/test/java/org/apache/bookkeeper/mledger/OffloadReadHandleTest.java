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
package org.apache.bookkeeper.mledger;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OffloadReadHandleTest extends MockedBookKeeperTestCase {

    @DataProvider(name = "flowPermits")
    public Object[][] permits() {
        return new Object[][]{
                {-1L},
                {0L},
                {50L},
                {100L},
                {10000L}
        };
    }

    @Test(dataProvider = "flowPermits")
    public void testFlowPermits(long flowPermits) throws Exception {
        OffloadReadHandle handle = (OffloadReadHandle) initializeReadHandle(flowPermits);
        try {
            long start = System.currentTimeMillis();
            handle.read(1, 1);
            handle.read(1, 1);
            handle.read(1, 1);
            handle.read(1, 1);
            handle.read(1, 1);

            long actualDuration = System.currentTimeMillis() - start;
            if (flowPermits <= 0L) {
                Assert.assertEquals(actualDuration, 4000D, 4000D);
            } else if (flowPermits == 50L) {
                long expectDuration = 8000;
                Assert.assertEquals(actualDuration, expectDuration, expectDuration * 0.2D);
            } else if (flowPermits == 100L) {
                long expectDuration = 4000;
                Assert.assertEquals(actualDuration, expectDuration, expectDuration * 0.2D);
            } else if (flowPermits == 10000L) {
                Assert.assertEquals(actualDuration, 1000D, 1000D);
            }
        } finally {
            handle.close();
            handle.reset();
        }
    }


    @Test
    public void testOffloadFlowPermitsMultiThreads() throws Exception {
        OffloadReadHandle handle = (OffloadReadHandle) initializeReadHandle(1000);
        AtomicBoolean failed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(10);
        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                new Thread(() -> {
                    for (int j = 0; j < 10; j++) {
                        try {
                            handle.read(1, 1);
                        } catch (Exception e) {
                            failed.set(true);
                        }
                    }
                    latch.countDown();
                }).start();
            }

            latch.await();

            Assert.assertFalse(failed.get());
            long actualDuration = System.currentTimeMillis() - start;
            long expectDuration = TimeUnit.SECONDS.toMillis(10);
            Assert.assertEquals(actualDuration, expectDuration, expectDuration * 0.2D);
        } finally {
            handle.close();
            handle.reset();
        }
    }


    private ReadHandle initializeReadHandle(long flowPermits) throws Exception {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(100);
        for (int a = 0; a < 100; a++) {
            buf.writeByte(0);
        }
        LedgerEntry entry = LedgerEntryImpl.create(1, 1, buf.readableBytes(), buf);
        List<LedgerEntry> entryList = Lists.newArrayList(entry);
        LedgerEntries entries = LedgerEntriesImpl.create(entryList);
        ReadHandle handle = Mockito.mock(ReadHandle.class);
        Mockito.doAnswer(inv -> CompletableFuture.completedFuture(entries)).when(handle)
                .readAsync(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doAnswer(inv -> {
            entries.close();
            return CompletableFuture.completedFuture(null);
        }).when(handle).closeAsync();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setManagedLedgerOffloadFlowPermitsPerSecond(flowPermits);

        CompletableFuture<ReadHandle> future = OffloadReadHandle.create(handle, config,
                MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder().setLedgerId(1)
                        .setEntries(1).setSize(100).build());

        return future.get();
    }
}