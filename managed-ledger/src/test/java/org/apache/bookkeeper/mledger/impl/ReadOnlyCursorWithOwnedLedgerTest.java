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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.util.Futures;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 10000)
public class ReadOnlyCursorWithOwnedLedgerTest {
    @DataProvider(name = "ledgerCloseFailures")
    public Object[][] ledgerCloseFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "ledgerCloseFailures")
    public void testCursorCloseFailureStillClosesOwnerOnceAndPreservesFailure(boolean ledgerCloseFails)
            throws Exception {
        ReadOnlyCursor cursor = mock(ReadOnlyCursor.class);
        ReadOnlyManagedLedger ledger = mock(ReadOnlyManagedLedger.class);
        ManagedLedgerException cursorFailure = new ManagedLedgerException("cursor close failed");
        doAnswer(invocation -> {
            AsyncCallbacks.CloseCallback callback = invocation.getArgument(0);
            callback.closeFailed(cursorFailure, invocation.getArgument(1));
            return null;
        }).when(cursor).asyncClose(any(), any());
        CompletableFuture<Void> ownerClose = new CompletableFuture<>();
        when(ledger.asyncClose()).thenReturn(ownerClose);
        ReadOnlyCursor owned = new ReadOnlyCursorWithOwnedLedger(cursor, ledger);
        Futures.CloseFuture firstClose = new Futures.CloseFuture();
        Futures.CloseFuture repeatedClose = new Futures.CloseFuture();

        owned.asyncClose(firstClose, null);
        owned.asyncClose(repeatedClose, null);

        verify(cursor).asyncClose(any(), any());
        verify(ledger).asyncClose();
        assertThat(firstClose).isNotDone();
        assertThat(repeatedClose).isNotDone();
        if (ledgerCloseFails) {
            ownerClose.completeExceptionally(new ManagedLedgerException("ledger close failed"));
        } else {
            ownerClose.complete(null);
        }
        assertThatThrownBy(() -> firstClose.get(5, TimeUnit.SECONDS)).hasCause(cursorFailure);
        assertThatThrownBy(() -> repeatedClose.get(5, TimeUnit.SECONDS)).hasCause(cursorFailure);

        Futures.CloseFuture laterClose = new Futures.CloseFuture();
        owned.asyncClose(laterClose, null);
        assertThatThrownBy(() -> laterClose.get(5, TimeUnit.SECONDS)).hasCause(cursorFailure);
        verify(cursor).asyncClose(any(), any());
        verify(ledger).asyncClose();
    }

    @Test
    public void testSynchronousCursorCloseFailureStillClosesOwner() throws Exception {
        ReadOnlyCursor cursor = mock(ReadOnlyCursor.class);
        ReadOnlyManagedLedger ledger = mock(ReadOnlyManagedLedger.class);
        doThrow(new IllegalStateException("synchronous cursor close failure"))
                .when(cursor).asyncClose(any(), any());
        when(ledger.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        ReadOnlyCursor owned = new ReadOnlyCursorWithOwnedLedger(cursor, ledger);
        Futures.CloseFuture closing = new Futures.CloseFuture();

        owned.asyncClose(closing, null);

        assertThatThrownBy(() -> closing.get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("synchronous cursor close failure");
        verify(ledger).asyncClose();
    }
}
