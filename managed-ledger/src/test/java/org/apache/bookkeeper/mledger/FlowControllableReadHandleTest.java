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
package org.apache.bookkeeper.mledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FlowControllableReadHandleTest {

    @Test
    public void test() throws Exception {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(100);
        for (int a = 0; a < 100; a++) {
            buf.writeByte(0);
        }
        LedgerEntry entry = LedgerEntryImpl.create(1, 1, buf.readableBytes(), buf);
        LedgerEntries entries = LedgerEntriesImpl.create(Collections.singletonList(entry));

        ReadHandle handle = Mockito.mock(ReadHandle.class);
        Mockito.doAnswer(inv -> CompletableFuture.completedFuture(entries)).when(handle).readAsync(1, 1);

        long start = System.currentTimeMillis();
        CompletableFuture<ReadHandle> future = FlowControllableReadHandle.create(handle, 100);
        ReadHandle h = future.get();
        h.readAsync(1, 1);
        h.readAsync(1, 1);
        h.readAsync(1, 1);
        h.readAsync(1, 1);
        h.readAsync(1, 1);
        h.readAsync(1, 1);
        h.readAsync(1, 1);
        h.readAsync(1, 1);
        h.readAsync(1, 1);
        h.readAsync(1, 1);

        Assert.assertTrue(System.currentTimeMillis() - start > TimeUnit.SECONDS.toMillis(8));
    }

}
