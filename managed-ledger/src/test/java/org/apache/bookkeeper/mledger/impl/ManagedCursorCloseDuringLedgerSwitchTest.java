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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class ManagedCursorCloseDuringLedgerSwitchTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 30000)
    public void testCloseWaitsForLedgerSwitchAndQueuedMarkDelete() throws Exception {
        String ledgerName = "close-during-cursor-ledger-switch";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.addEntry("first".getBytes());
        Position acknowledged = ledger.addEntry("acknowledged".getBytes());

        // Hold the add that initializes the new cursor ledger. The cursor stays in SwitchingLedger until released.
        CompletableFuture<Void> releaseLedgerSwitch = bkc.addEntryResponseGate();
        CompletableFuture<Void> releaseQueuedMarkDelete = bkc.addEntryResponseGate();
        CompletableFuture<Void> markDeleteFuture = new CompletableFuture<>();
        cursor.asyncMarkDelete(acknowledged, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                markDeleteFuture.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                markDeleteFuture.completeExceptionally(exception);
            }
        }, null);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> cursor.state == ManagedCursorImpl.State.SwitchingLedger);

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                closeFuture.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                closeFuture.completeExceptionally(exception);
            }
        }, null);

        boolean closeCompletedDuringSwitch;
        boolean closeCompletedWithQueuedMarkDelete;
        try {
            closeCompletedDuringSwitch = completesWithinOneSecond(closeFuture);
            releaseLedgerSwitch.complete(null);
            closeCompletedWithQueuedMarkDelete = completesWithinOneSecond(closeFuture);
        } finally {
            releaseLedgerSwitch.complete(null);
            releaseQueuedMarkDelete.complete(null);
        }

        assertThat(closeCompletedDuringSwitch)
                .as("close must wait for the cursor-ledger switch")
                .isFalse();
        assertThat(closeCompletedWithQueuedMarkDelete)
                .as("close must wait for the mark-delete queued behind the switch")
                .isFalse();
        markDeleteFuture.get(10, TimeUnit.SECONDS);
        closeFuture.get(10, TimeUnit.SECONDS);

        CompletableFuture<ManagedCursorInfo> cursorInfoFuture = new CompletableFuture<>();
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), cursor.getName(),
                new MetaStoreCallback<ManagedCursorInfo>() {
                    @Override
                    public void operationComplete(ManagedCursorInfo result, Stat stat) {
                        cursorInfoFuture.complete(result);
                    }

                    @Override
                    public void operationFailed(MetaStoreException exception) {
                        cursorInfoFuture.completeExceptionally(exception);
                    }
                });
        ManagedCursorInfo cursorInfo = cursorInfoFuture.get(10, TimeUnit.SECONDS);
        assertThat(cursorInfo.getMarkDeleteLedgerId()).isEqualTo(acknowledged.getLedgerId());
        assertThat(cursorInfo.getMarkDeleteEntryId()).isEqualTo(acknowledged.getEntryId());
        assertThat(cursorInfo.getCursorsLedgerId()).isEqualTo(-1);
        ledger.close();
    }

    private boolean completesWithinOneSecond(CompletableFuture<Void> future) throws Exception {
        try {
            future.get(1, TimeUnit.SECONDS);
            return true;
        } catch (TimeoutException expected) {
            return false;
        }
    }
}
