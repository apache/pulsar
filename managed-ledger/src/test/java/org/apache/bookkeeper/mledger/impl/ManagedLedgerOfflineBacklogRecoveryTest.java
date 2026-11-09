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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerOfflineBacklogRecoveryTest {
    private static final long CURSOR_LEDGER_ID = 17;
    private static final byte[] PASSWORD = "cursor-password".getBytes(StandardCharsets.UTF_8);
    private ManagedLedgerFactoryImpl factory;
    private BookKeeper bookKeeper;
    private OpenBuilder builder;
    private ReadHandle handle;

    @BeforeMethod
    public void setUp() {
        factory = mock(ManagedLedgerFactoryImpl.class, CALLS_REAL_METHODS);
        bookKeeper = mock(BookKeeper.class);
        builder = mock(OpenBuilder.class, RETURNS_SELF);
        handle = mock(ReadHandle.class);
        when(bookKeeper.newOpenLedgerOp()).thenReturn(builder);
        when(builder.execute()).thenReturn(CompletableFuture.completedFuture(handle));
        when(handle.getId()).thenReturn(CURSOR_LEDGER_ID);
        when(handle.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    public void testRecoveryReadsPositionsAndClosesResources() {
        ByteBuf first = positionBuffer(3, 4);
        ByteBuf last = positionBuffer(3, 8);
        when(handle.getLastAddConfirmed()).thenReturn(1L);
        when(handle.readUnconfirmedAsync(0, 0)).thenReturn(CompletableFuture.completedFuture(entries(0, first)));
        when(handle.readUnconfirmedAsync(1, 1)).thenReturn(CompletableFuture.completedFuture(entries(1, last)));

        assertThat(recover()).isEqualTo(PositionFactory.create(3, 8));

        assertThat(first.refCnt()).isZero();
        assertThat(last.refCnt()).isZero();
        verify(handle).closeAsync();
        verify(builder).withRecovery(false);
        verify(builder).withLedgerId(CURSOR_LEDGER_ID);
        verify(builder).withDigestType(DigestType.CRC32C);
        verify(builder).withPassword(PASSWORD);
    }

    @Test
    public void testSynchronousCloseFailurePreservesRecoveredPosition() {
        ByteBuf buffer = positionBuffer(3, 4);
        when(handle.getLastAddConfirmed()).thenReturn(0L);
        when(handle.readUnconfirmedAsync(0, 0)).thenReturn(CompletableFuture.completedFuture(entries(0, buffer)));
        when(handle.closeAsync()).thenThrow(new IllegalStateException("injected close failure"));

        assertThat(recover()).isEqualTo(PositionFactory.create(3, 4));

        assertThat(buffer.refCnt()).isZero();
        verify(handle).closeAsync();
    }

    @Test
    public void testMalformedPositionDoesNotReturnPartialRecovery() {
        ByteBuf first = positionBuffer(3, 4);
        ByteBuf malformed = Unpooled.wrappedBuffer(new byte[] {(byte) 0xff});
        when(handle.getLastAddConfirmed()).thenReturn(1L);
        when(handle.readUnconfirmedAsync(0, 0)).thenReturn(CompletableFuture.completedFuture(entries(0, first)));
        when(handle.readUnconfirmedAsync(1, 1))
                .thenReturn(CompletableFuture.completedFuture(entries(1, malformed)));

        assertThat(recover()).isNull();

        assertThat(first.refCnt()).isZero();
        assertThat(malformed.refCnt()).isZero();
        verify(handle).closeAsync();
    }

    @Test
    public void testNoSuchEntryEndsRecoveryAndClosesHandle() {
        ByteBuf first = positionBuffer(3, 4);
        when(handle.getLastAddConfirmed()).thenReturn(1L);
        when(handle.readUnconfirmedAsync(0, 0)).thenReturn(CompletableFuture.completedFuture(entries(0, first)));
        when(handle.readUnconfirmedAsync(1, 1)).thenReturn(CompletableFuture.failedFuture(
                BKException.create(BKException.Code.NoSuchEntryException)));

        assertThat(recover()).isEqualTo(PositionFactory.create(3, 4));

        assertThat(first.refCnt()).isZero();
        verify(handle).closeAsync();
    }

    @DataProvider(name = "interruptedOperation")
    public Object[][] interruptedOperation() {
        return new Object[][] {{"open"}, {"read"}};
    }

    @Test(dataProvider = "interruptedOperation", timeOut = 15000)
    public void testInterruptedRecoveryClosesLateResult(String operation) throws Exception {
        CompletableFuture<ReadHandle> open = new CompletableFuture<>();
        CompletableFuture<LedgerEntries> read = new CompletableFuture<>();
        CountDownLatch operationStarted = new CountDownLatch(1);
        ByteBuf buffer = positionBuffer(3, 4);
        LedgerEntries entries = entries(0, buffer);
        if ("open".equals(operation)) {
            when(builder.execute()).thenAnswer(invocation -> {
                operationStarted.countDown();
                return open;
            });
        } else {
            when(handle.getLastAddConfirmed()).thenReturn(0L);
            when(handle.readUnconfirmedAsync(0, 0)).thenAnswer(invocation -> {
                operationStarted.countDown();
                return read;
            });
        }
        CompletableFuture<Position> result = new CompletableFuture<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread recoveryThread = new Thread(() -> {
            Position position = recover();
            interrupted.set(Thread.currentThread().isInterrupted());
            result.complete(position);
        }, "offline-backlog-recovery");
        recoveryThread.start();
        try {
            assertThat(operationStarted.await(5, TimeUnit.SECONDS)).isTrue();
            recoveryThread.interrupt();
            assertThat(result.get(5, TimeUnit.SECONDS)).isNull();
            assertThat(interrupted).isTrue();
            if ("open".equals(operation)) {
                verify(handle, never()).closeAsync();
                open.complete(handle);
                verify(handle, never()).readUnconfirmedAsync(0, 0);
            } else {
                assertThat(buffer.refCnt()).isEqualTo(1);
                read.complete(entries);
                assertThat(buffer.refCnt()).isZero();
            }
            verify(handle).closeAsync();
        } finally {
            recoveryThread.interrupt();
            open.complete(handle);
            read.complete(entries);
            recoveryThread.join(TimeUnit.SECONDS.toMillis(5));
            if (buffer.refCnt() > 0) {
                entries.close();
            }
        }
        assertThat(recoveryThread.isAlive()).isFalse();
    }

    private Position recover() {
        return factory.tryGetMDPosition(bookKeeper, CURSOR_LEDGER_ID, "subscription",
                BookKeeper.DigestType.CRC32C, PASSWORD);
    }

    private static ByteBuf positionBuffer(long ledgerId, long entryId) {
        return Unpooled.wrappedBuffer(new PositionInfo().setLedgerId(ledgerId).setEntryId(entryId).toByteArray());
    }

    private static LedgerEntries entries(long entryId, ByteBuf buffer) {
        return LedgerEntriesImpl.create(new ArrayList<>(List.of(
                LedgerEntryImpl.create(CURSOR_LEDGER_ID, entryId, buffer.readableBytes(), buffer))));
    }
}
