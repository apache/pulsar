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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ReadOnlyManagedLedgerImplTest extends MockedBookKeeperTestCase {
    private static final String MANAGED_LEDGER_NAME_NON_PROPERTIES = "ml-non-properties";
    private static final String MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES = "ml-attached-properties";


    @Test
    public void testReadOnlyManagedLedgerImplAttachProperties()
            throws ManagedLedgerException, InterruptedException, ExecutionException, TimeoutException {
        final ManagedLedger ledger = factory.open(MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES,
                defaultConfig().setRetentionTime(1, TimeUnit.HOURS));
        final String propertiesKey = "test-key";
        final String propertiesValue = "test-value";

        ledger.setConfig(defaultConfig());
        ledger.addEntry("entry-0".getBytes());
        Map<String, String> properties = new HashMap<>();
        properties.put(propertiesKey, propertiesValue);
        ledger.setProperties(Collections.unmodifiableMap(properties));
        CompletableFuture<Void> future = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES,
                new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
                    @Override
                    public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger,
                                                                  Object ctx) {
                        managedLedger.getProperties().forEach((key, value) -> {
                            assertEquals(key, propertiesKey);
                            assertEquals(value, propertiesValue);
                        });
                        future.complete(null);
                    }

                    @Override
                    public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, defaultConfig(), null);

        future.get(60, TimeUnit.SECONDS);
    }

    @Test
    public void testReadOnlyManagedLedgerImplNoProperties()
            throws ManagedLedgerException, InterruptedException, ExecutionException, TimeoutException {
        final ManagedLedger ledger = factory.open(MANAGED_LEDGER_NAME_NON_PROPERTIES,
                defaultConfig().setRetentionTime(1, TimeUnit.HOURS));
        ledger.setConfig(defaultConfig());
        ledger.addEntry("entry-0".getBytes());
        CompletableFuture<Void> future = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(MANAGED_LEDGER_NAME_NON_PROPERTIES,
                new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
                    @Override
                    public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger,
                                                                  Object ctx) {
                        assertEquals(managedLedger.getProperties().size(), 0);
                        future.complete(null);
                    }

                    @Override
                    public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, defaultConfig(), null);

        future.get(60, TimeUnit.SECONDS);
    }

    /**
     * Verify that {@link ManagedLedgerImpl#asyncClose} drains cached read handles
     * in both the {@code lh == null} (no write ledger) and {@code lh != null}
     * branches, and that the state transitions to Closed.
     *
     * <p>Regression for PR #26918 – the {@code lh == null} branch previously
     * skipped {@code ledgerCache.forEach(invalidateReadHandle)}.
     */
    @Test(timeOut = 60000)
    public void testCloseReleasesCachedReadHandlesNoWriteLedger() throws Exception {
        ManagedLedgerConfig cfg = defaultConfig();
        cfg.setMaxEntriesPerLedger(2);
        cfg.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        ManagedLedger ledger = factory.open("test-close-drain-no-lh", cfg);

        // Add enough entries to trigger at least one rollover, creating multiple ledgers.
        for (int i = 0; i < 5; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }

        ManagedLedgerImpl impl = (ManagedLedgerImpl) ledger;

        // Wait for rollover to settle – ledgers map should have at least 2 entries.
        long start = System.currentTimeMillis();
        while (impl.ledgers.size() < 2 && System.currentTimeMillis() - start < 10000) {
            Thread.sleep(50);
        }
        assertTrue(impl.ledgers.size() >= 2, "Expected at least 2 ledgers for this test");

        // Populate ledgerCache by opening a read handle on a historical ledger.
        long firstLedgerId = impl.ledgers.firstKey();
        CompletableFuture<ReadHandle> hf = impl.getLedgerHandle(firstLedgerId);
        hf.get(30, TimeUnit.SECONDS);
        assertEquals(impl.ledgerCache.size(), 1, "Cache should contain the historical handle");

        // Close the managed ledger asynchronously.
        CountDownLatch closeLatch = new CountDownLatch(1);
        impl.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                closeLatch.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                fail("Close failed: " + exception.getMessage());
                closeLatch.countDown();
            }
        }, null);
        assertTrue(closeLatch.await(30, TimeUnit.SECONDS), "Close did not complete in time");

        // After close, the cache must be empty regardless of the branch taken.
        assertEquals(impl.ledgerCache.size(), 0,
                "ledgerCache should be drained after asyncClose");
        assertEquals(impl.getState(), State.Closed, "State should be Closed");
    }

    /**
     * Verify that {@link ManagedLedgerImpl#getLedgerHandle} rejects
     * handle admission after the managed ledger has transitioned to Closed.
     *
     * <p>Regression for PR #26918 – a read started before close could pause
     * before handle acquisition, let asyncClose drain the cache, then insert
     * a handle after state was already Closed.
     */
    @Test(timeOut = 60000)
    public void testGetLedgerHandleRejectsAfterClose() throws Exception {
        ManagedLedgerConfig cfg = defaultConfig();
        cfg.setMaxEntriesPerLedger(2);
        cfg.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        ManagedLedger ledger = factory.open("test-getHandle-reject-after-close", cfg);

        for (int i = 0; i < 5; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }

        ManagedLedgerImpl impl = (ManagedLedgerImpl) ledger;

        // Wait for rollover to settle.
        long start = System.currentTimeMillis();
        while (impl.ledgers.size() < 2 && System.currentTimeMillis() - start < 10000) {
            Thread.sleep(50);
        }

        // First read to populate the cache with one legitimate handle.
        long firstLedgerId = impl.ledgers.firstKey();
        impl.getLedgerHandle(firstLedgerId).get(30, TimeUnit.SECONDS);
        assertEquals(impl.ledgerCache.size(), 1);

        // Close the managed ledger asynchronously.
        CountDownLatch closeLatch = new CountDownLatch(1);
        impl.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                closeLatch.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                closeLatch.countDown();
            }
        }, null);
        assertTrue(closeLatch.await(30, TimeUnit.SECONDS));
        assertEquals(impl.getState(), State.Closed);
        assertEquals(impl.ledgerCache.size(), 0);

        // Now attempt a new getLedgerHandle – it must fail rather than silently
        // inserting a handle into the cache.
        long anotherLedgerId = impl.ledgers.lastKey();
        CompletableFuture<ReadHandle> future = impl.getLedgerHandle(anotherLedgerId);
        assertTrue(future.isDone(),
                "Future should be completed (either normally or exceptionally)");
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected getLedgerHandle to fail after close");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof ManagedLedgerException,
                    "Expected ManagedLedgerException, got " + ee.getCause());
        }

        // Cache must still be empty – no late insertions after close.
        assertEquals(impl.ledgerCache.size(), 0);
    }

    /**
     * Verify that {@link ReadOnlyManagedLedger#close()} retains backward
     * compatibility via the default no-op method declared on the interface,
     * and that the built-in wrapper still delegates cleanup correctly.
     *
     * <p>Regression for PR #26918 – adding a mandatory abstract {@code close()}
     * would break PIP-384 custom managed-ledger backends.
     */
    @Test(timeOut = 60000)
    public void testReadOnlyManagedLedgerCloseBackwardCompatibility() throws Exception {
        ManagedLedgerConfig cfg = defaultConfig();
        ManagedLedger ledger = factory.open("test-ro-ml-compat", cfg);
        ledger.addEntry("entry".getBytes());

        // Obtain the ReadOnlyManagedLedger via the factory wrapper path
        // (this is the production path exercised by snapshot recovery).
        CompletableFuture<ReadOnlyManagedLedger> openFuture = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger("test-ro-ml-compat",
                new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
                    @Override
                    public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger, Object ctx) {
                        openFuture.complete(managedLedger);
                    }

                    @Override
                    public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        openFuture.completeExceptionally(exception);
                    }
                }, cfg, null);

        ReadOnlyManagedLedger roLedger = openFuture.get(30, TimeUnit.SECONDS);
        // close() should be callable and release underlying resources.
        roLedger.close();

        // Custom implementations without an explicit close() override must also work –
        // the default method on the interface is a no-op and does not throw.
        ReadOnlyManagedLedger customImpl = new ReadOnlyManagedLedger() {
            @Override
            public void asyncReadEntry(org.apache.bookkeeper.mledger.Position position,
                    AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getNumberOfEntries() { return 0; }

            @Override
            public ReadOnlyCursor createReadOnlyCursor(org.apache.bookkeeper.mledger.Position position) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Map<String, String> getProperties() { return Collections.emptyMap(); }
            // NOTE: close() intentionally NOT overridden – exercises the default no-op.
        };
        customImpl.close(); // must not throw
    }

}
