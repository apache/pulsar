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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/** Verifies that the time since which a cursor is idle is persisted and kept when the cursor is recovered. */
@Test(timeOut = 30000)
public class ManagedCursorIdleTest extends MockedBookKeeperTestCase {

    @Test
    public void testIdleTimeIsKeptWhenTheLedgerIsReopened() throws Exception {
        ManagedLedger ledger = factory.open("idle-reopened", new ManagedLedgerConfig());
        ManagedCursor cursor = ledger.openCursor("sub");
        cursor.markIdle();
        long idleSince = cursor.getLastActive();
        Thread.sleep(10);
        // An idle cursor stays idle since the same time
        cursor.markIdle();
        assertThat(cursor.getLastActive()).isEqualTo(idleSince);

        ledger.close();
        Thread.sleep(10);
        ledger = factory.open("idle-reopened", new ManagedLedgerConfig());
        ManagedCursorImpl recovered = (ManagedCursorImpl) ledger.openCursor("sub");
        assertThat(recovered.getLastActive()).isEqualTo(idleSince);

        // Once used again, the cursor has to be persisted, or it would be recovered as idle after a crash
        assertThat(recovered.isDirty()).isFalse();
        recovered.updateLastActive();
        assertThat(recovered.getLastActive()).isGreaterThan(idleSince);
        assertThat(recovered.isDirty()).isTrue();
        ledger.close();
    }

    @Test
    public void testIdleTimeIsKeptWhenTheCursorIsRecoveredFromItsLedger() throws Exception {
        ManagedLedgerImpl ledger =
                (ManagedLedgerImpl) factory.open("idle-recovered-from-ledger", new ManagedLedgerConfig());
        ManagedCursor cursor = ledger.openCursor("sub");
        Position first = ledger.addEntry("entry-1".getBytes());
        Position second = ledger.addEntry("entry-2".getBytes());
        // The cursor ledger is created while the cursor is in use
        cursor.markDelete(first);
        cursor.markIdle();
        long idleSince = cursor.getLastActive();
        cursor.markDelete(second);
        assertThat(storedCursorInfo(ledger, "sub").hasIdleSince()).isFalse();
        Thread.sleep(10);

        // Without closing the ledger, as after a crash, the cursor is recovered from its ledger, which alone records
        // that the cursor is idle
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedCursor recovered =
                factory2.open("idle-recovered-from-ledger", new ManagedLedgerConfig()).openCursor("sub");
        assertThat(recovered.getLastActive()).isEqualTo(idleSince);
    }

    @Test
    public void testCursorInUseIsIdleSinceItsRecovery() throws Exception {
        ManagedLedger ledger = factory.open("in-use-recovered", new ManagedLedgerConfig());
        ManagedCursor cursor = ledger.openCursor("sub");
        cursor.updateLastActive();
        long lastActive = cursor.getLastActive();
        ledger.close();
        Thread.sleep(10);

        // The consumers of the cursor were connected until it was closed: the cursor is idle since its recovery,
        // until they come back
        ledger = factory.open("in-use-recovered", new ManagedLedgerConfig());
        long recoveredAt = ledger.openCursor("sub").getLastActive();
        assertThat(recoveredAt).isGreaterThan(lastActive);

        // If they never come back, it stays idle since then
        ledger.close();
        Thread.sleep(10);
        ledger = factory.open("in-use-recovered", new ManagedLedgerConfig());
        assertThat(ledger.openCursor("sub").getLastActive()).isEqualTo(recoveredAt);
        ledger.close();
    }

    @Test
    public void testCursorInUseHasNothingToPersistWhenItsConsumersComeBack() throws Exception {
        ManagedLedger ledger = factory.open("in-use-used-again", new ManagedLedgerConfig());
        ledger.openCursor("sub").updateLastActive();
        ledger.close();

        ledger = factory.open("in-use-used-again", new ManagedLedgerConfig());
        ManagedCursorImpl recovered = (ManagedCursorImpl) ledger.openCursor("sub");
        recovered.updateLastActive();
        assertThat(recovered.isDirty()).isFalse();
        ledger.close();
    }

    @Test
    public void testCursorUsedAgainWhilePersistedAsIdleIsPersistedAgain() throws Exception {
        ManagedLedger ledger = factory.open("used-while-persisted-as-idle", new ManagedLedgerConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position position = ledger.addEntry("entry".getBytes());
        cursor.markDelete(position);
        cursor.markIdle();

        // The cursor is used again after its idle state is serialized, before it is written
        bkc.addEntryResponseDelay(1, TimeUnit.SECONDS);
        CompletableFuture<Void> markDelete = new CompletableFuture<>();
        cursor.asyncMarkDelete(position, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                markDelete.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                markDelete.completeExceptionally(exception);
            }
        }, null);
        cursor.updateLastActive();
        assertThat(cursor.isDirty()).isFalse();
        markDelete.get(5, TimeUnit.SECONDS);
        assertThat(lastPersistedPosition(cursor).hasIdleSince()).isTrue();
        assertThat(cursor.isDirty()).isTrue();

        long persistedEntry = cursor.cursorLedger.getLastAddConfirmed();
        cursor.flush();
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> cursor.cursorLedger.getLastAddConfirmed() > persistedEntry);
        assertThat(lastPersistedPosition(cursor).hasIdleSince()).isFalse();
        ledger.close();
    }

    private static ManagedCursorInfo storedCursorInfo(ManagedLedgerImpl ledger, String cursorName) throws Exception {
        CompletableFuture<ManagedCursorInfo> result = new CompletableFuture<>();
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), cursorName, new MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedCursorInfo info, Stat stat) {
                result.complete(info);
            }

            @Override
            public void operationFailed(MetaStoreException exception) {
                result.completeExceptionally(exception);
            }
        });
        return result.get(5, TimeUnit.SECONDS);
    }

    private static PositionInfo lastPersistedPosition(ManagedCursorImpl cursor) throws Exception {
        CompletableFuture<PositionInfo> result = new CompletableFuture<>();
        long entryId = cursor.cursorLedger.getLastAddConfirmed();
        cursor.cursorLedger.asyncReadEntries(entryId, entryId, (rc, lh, entries, ctx) -> {
            try {
                if (rc != 0) {
                    throw new IllegalStateException("Failed to read cursor ledger entry: " + rc);
                }
                result.complete(PositionInfo.parseFrom(entries.nextElement().getEntry()));
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        }, null);
        return result.get(5, TimeUnit.SECONDS);
    }
}
