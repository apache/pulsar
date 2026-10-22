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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.CursorInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.MessageRangeInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerFactoryTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    public void testGetManagedLedgerInfoWithClose() throws Exception {
        ManagedLedgerConfig conf = defaultConfig();
        conf.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testGetManagedLedgerInfo", conf);
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry1".getBytes());
        Position p2 = ledger.addEntry("entry2".getBytes());
        Position p3 = ledger.addEntry("entry3".getBytes());
        ledger.addEntry("entry4".getBytes());

        c1.delete(p2);
        c1.delete(p3);

        ledger.close();

        ManagedLedgerInfo info = factory.getManagedLedgerInfo("testGetManagedLedgerInfo");

        assertEquals(info.ledgers.size(), 5);

        assertEquals(info.ledgers.get(0).ledgerId, 3);
        assertEquals(info.ledgers.get(1).ledgerId, 4);
        assertEquals(info.ledgers.get(2).ledgerId, 5);
        assertEquals(info.ledgers.get(3).ledgerId, 6);

        for (ManagedLedgerInfo.LedgerInfo linfo : info.ledgers) {
            assertNotNull(linfo.timestamp);
        }

        assertEquals(info.cursors.size(), 1);

        CursorInfo cursorInfo = info.cursors.get("c1");
        assertEquals(cursorInfo.markDelete.ledgerId, 3);
        assertEquals(cursorInfo.markDelete.entryId, -1);

        assertEquals(cursorInfo.individualDeletedMessages.size(), 2);

        MessageRangeInfo mri = cursorInfo.individualDeletedMessages.get(0);
        assertEquals(mri.from.ledgerId, p2.getLedgerId());
        assertEquals(mri.from.entryId, -1);
        assertEquals(mri.to.ledgerId, p2.getLedgerId());
        assertEquals(mri.to.entryId, 0);
    }

    @Test(timeOut = 30000)
    public void testDeleteCursorLedgerMetadataFailure() throws Exception {
        String name = "delete-cursor-failure";
        long cursorLedgerId = prepareClosedLedgerWithCursor(name);
        // The ZooKeeper ledger manager reports metadata deletion failures (such as connection loss) as ZKException.
        injectCursorDeleteFailure(cursorLedgerId, BKException.Code.ZKException);

        CompletableFuture<Void> deletion = deleteAsync(name);
        assertThatThrownBy(() -> deletion.get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(BKException.ZKException.class);
        verify(bkc).asyncDeleteLedger(eq(cursorLedgerId), any(), any());
        verify(metadataStore, never()).delete(eq("/managed-ledgers/" + name + "/c1"), any());
        assertThat(factory.getManagedLedgerInfo(name).cursors).containsKey("c1");
    }

    @DataProvider(name = "cursorDeleteResults")
    public Object[][] cursorDeleteResults() {
        return new Object[][] {
                {BKException.Code.OK},
                {BKException.Code.NoSuchLedgerExistsException},
                {BKException.Code.NoSuchLedgerExistsOnMetadataServerException}
        };
    }

    @Test(dataProvider = "cursorDeleteResults", timeOut = 30000)
    public void testDeleteCursorLedgerSuccess(int result) throws Exception {
        String name = "delete-cursor-success";
        long cursorLedgerId = prepareClosedLedgerWithCursor(name);
        if (result != BKException.Code.OK) {
            bkc.deleteLedger(cursorLedgerId);
            injectCursorDeleteFailure(cursorLedgerId, result);
        }

        deleteAsync(name).get(5, TimeUnit.SECONDS);
        verify(bkc).asyncDeleteLedger(eq(cursorLedgerId), any(), any());
        verify(metadataStore).delete(eq("/managed-ledgers/" + name + "/c1"), eq(Optional.empty()));
        assertThat(metadataStore.exists("/managed-ledgers/" + name).get(5, TimeUnit.SECONDS)).isFalse();
        assertThat(bkc.getLedgers()).doesNotContain(cursorLedgerId);
    }

    private long prepareClosedLedgerWithCursor(String name) throws Exception {
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        bkc = spy(bkc);
        metadataStore = spy(metadataStore);
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = defaultConfig().setMaxUnackedRangesToPersistInMetadataStore(0)
                .setThrottleMarkDelete(0);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry(new byte[] {1});
        Position position = ledger.addEntry(new byte[] {2});
        cursor.delete(position);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> cursor.getStats().getPersistLedgerSucceed() > 0);
        ledger.close();
        assertThat(factory.ledgers).doesNotContainKey(name);
        long cursorLedgerId = factory.getManagedLedgerInfo(name).cursors.get("c1").cursorsLedgerId;
        assertThat(cursorLedgerId).isNotEqualTo(-1L);
        return cursorLedgerId;
    }

    private void injectCursorDeleteFailure(long cursorLedgerId, int result) {
        doAnswer(invocation -> {
            DeleteCallback callback = invocation.getArgument(1);
            callback.deleteComplete(result, invocation.getArgument(2));
            return null;
        }).when(bkc).asyncDeleteLedger(eq(cursorLedgerId), any(), any());
    }

    private CompletableFuture<Void> deleteAsync(String name) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        factory.asyncDelete(name, new DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                result.complete(null);
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }

    /**
     * see: https://github.com/apache/pulsar/pull/18688.
     */
    @Test
    public void testConcurrentCloseLedgerAndSwitchLedgerForReproduceIssue() throws Exception {
        String managedLedgerName = "lg_" + UUID.randomUUID().toString().replaceAll("-", "_");

        ManagedLedgerConfig config = defaultConfig();
        config.setThrottleMarkDelete(1);
        config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
        config.setMaxEntriesPerLedger(5);

        // create managedLedger once and close it.
        ManagedLedgerImpl managedLedger1 = (ManagedLedgerImpl) factory.open(managedLedgerName, config);
        waitManagedLedgerStateEquals(managedLedger1, ManagedLedgerImpl.State.LedgerOpened);
        managedLedger1.close();

        // create managedLedger the second time.
        ManagedLedgerImpl managedLedger2 = (ManagedLedgerImpl) factory.open(managedLedgerName, config);
        waitManagedLedgerStateEquals(managedLedger2, ManagedLedgerImpl.State.LedgerOpened);

        // Mock the task create ledger complete now, it will change the state to another value which not is Closed.
        // Close managedLedger1 the second time.
        managedLedger1.createComplete(1, null, null);
        managedLedger1.close();

        // Verify managedLedger2 is still there.
        Assert.assertFalse(factory.ledgers.isEmpty());
        Assert.assertEquals(factory.ledgers.get(managedLedger2.getName()).join(), managedLedger2);

        // cleanup.
        managedLedger2.close();
    }

    @Test(timeOut = 30000)
    public void testShutdownWaitsForManagedLedgersToClose() throws Exception {
        CompletableFuture<Void> closeRequested = new CompletableFuture<>();
        CompletableFuture<Void> closeGate = new CompletableFuture<>();
        replaceFactoryWithGatedClose("ml", closeRequested, closeGate);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("ml");

        CompletableFuture<Void> shutdown = factory.shutdownAsync();
        closeRequested.get(5, TimeUnit.SECONDS);
        Awaitility.await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                .until(() -> !shutdown.isDone());

        closeGate.complete(null);
        shutdown.get(5, TimeUnit.SECONDS);
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.Closed);
    }

    @Test(timeOut = 30000)
    public void testShutdownDoesNotWaitForeverForStuckManagedLedgerClose() throws Exception {
        CompletableFuture<Void> closeRequested = new CompletableFuture<>();
        replaceFactoryWithGatedClose("ml", closeRequested, new CompletableFuture<>());
        factory.setShutdownLedgerCloseTimeoutMillis(100);
        factory.open("ml");

        // The close never completes, so the shutdown completes once it times out
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        assertTrue(closeRequested.isDone());
    }

    @Test(timeOut = 30000)
    public void testShutdownFailsOpenThatNeverStartedInitializing() throws Exception {
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        // The BookKeeper client never becomes available, so the open never starts initializing the ledger
        factory = new ManagedLedgerFactoryImpl(metadataStore, policyConfig -> new CompletableFuture<>(),
                new ManagedLedgerFactoryConfig());
        factory.setShutdownLedgerCloseTimeoutMillis(100);
        CompletableFuture<ManagedLedger> open = new CompletableFuture<>();
        factory.asyncOpen("ml", new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                open.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                open.completeExceptionally(exception);
            }
        }, null);

        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        assertThatThrownBy(() -> open.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFactoryClosedException.class);
    }

    @Test(timeOut = 30000)
    public void testShutdownClosesManagedLedgersAddedDuringShutdown() throws Exception {
        CompletableFuture<Void> closeRequested = new CompletableFuture<>();
        CompletableFuture<Void> closeGate = new CompletableFuture<>();
        replaceFactoryWithGatedClose("ml", closeRequested, closeGate);
        factory.open("ml");
        ManagedLedgerImpl lateLedger = (ManagedLedgerImpl) factory.open("late-ml");
        // Simulate an open that passed the closed check before the shutdown started, but added its ledger only
        // after the shutdown had listed the ledgers to close
        CompletableFuture<ManagedLedgerImpl> lateLedgerFuture = factory.ledgers.remove("late-ml");

        CompletableFuture<Void> shutdown = factory.shutdownAsync();
        closeRequested.get(5, TimeUnit.SECONDS);
        factory.ledgers.put("late-ml", lateLedgerFuture);
        closeGate.complete(null);

        shutdown.get(5, TimeUnit.SECONDS);
        assertEquals(lateLedger.getState(), ManagedLedgerImpl.State.Closed);
        assertThat(factory.ledgers).isEmpty();
    }

    /**
     * Replaces the factory with one whose managed ledger {@code gatedName} only closes once {@code closeGate}
     * completes; {@code closeRequested} completes when its close is requested.
     */
    private void replaceFactoryWithGatedClose(String gatedName, CompletableFuture<Void> closeRequested,
                                              CompletableFuture<Void> closeGate) throws Exception {
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                                                            ManagedLedgerConfig config,
                                                            Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
                if (!name.equals(gatedName)) {
                    return super.createManagedLedger(bk, store, name, config, mlOwnershipChecker);
                }
                return new ManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name, mlOwnershipChecker) {
                    @Override
                    public synchronized void asyncClose(CloseCallback callback, Object ctx) {
                        closeRequested.complete(null);
                        closeGate.thenRun(() -> super.asyncClose(callback, ctx));
                    }
                };
            }
        };
    }

    private void waitManagedLedgerStateEquals(ManagedLedgerImpl managedLedger, ManagedLedgerImpl.State expectedStat){
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(managedLedger.getState() == expectedStat));
    }

}
