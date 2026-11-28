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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.util.FutureUtil;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ManagedCursorUpdatePropertiesTest extends MockedBookKeeperTestCase {

    private ManagedLedger ledger;
    private ManagedCursor cursor;

    @Override
    protected void setUpTestCase() throws Exception {
        ledger = factory.open("update-cursor-properties-test-ledger");
        cursor = ledger.openCursor("c1");
    }

    @Test
    public void testUpdateCursorPropertiesAppliesTheWholeFunction() throws Exception {
        assertThat(cursor.putCursorProperty("keep", "1")).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(cursor.putCursorProperty("drop", "1")).succeedsWithin(5, TimeUnit.SECONDS);

        assertThat(cursor.updateCursorProperties(properties -> {
            properties.put("added", "2");
            properties.put("keep", "3");
            properties.remove("drop");
            return properties;
        })).succeedsWithin(5, TimeUnit.SECONDS);

        assertThat(cursor.getCursorProperties())
                .containsEntry("keep", "3")
                .containsEntry("added", "2")
                .doesNotContainKey("drop");
    }

    @Test
    public void testConditionalRemovalOnlyAppliesWhileValueMatches() throws Exception {
        assertThat(cursor.putCursorProperty("k", "a")).succeedsWithin(5, TimeUnit.SECONDS);

        assertThat(cursor.updateCursorProperties(conditionalRemoval("k", "other")))
                .succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(cursor.getCursorProperties()).containsEntry("k", "a");

        assertThat(cursor.updateCursorProperties(conditionalRemoval("k", "a")))
                .succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(cursor.getCursorProperties()).doesNotContainKey("k");
    }

    @Test
    public void testRemoveMissingPropertyRejectsStaleMetadataVersion() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory externalFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedCursor externalCursor = externalFactory.open(ledger.getName()).openCursor(cursor.getName());
        assertThat(externalCursor.putCursorProperty("k", "value")).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(cursor.getCursorProperties()).doesNotContainKey("k");

        assertThat(cursor.removeCursorProperty("k")).failsWithin(5, TimeUnit.SECONDS)
                .withThrowableThat()
                .extracting(FutureUtil::unwrapCompletionException)
                .isInstanceOf(ManagedLedgerException.BadVersionException.class);
        assertThat(factory.getCursorPropertiesAsync(ledger.getName(), cursor.getName()).get(5, TimeUnit.SECONDS))
                .containsEntry("k", "value");
    }

    private static Function<Map<String, String>, Map<String, String>> conditionalRemoval(
            String key, String expectedValue) {
        return properties -> {
            if (expectedValue.equals(properties.get(key))) {
                properties.remove(key);
            }
            return properties;
        };
    }

    @Test
    public void testUpdateCursorPropertiesOnEmptyProperties() throws Exception {
        assertThat(cursor.getCursorProperties()).isEmpty();

        assertThat(cursor.updateCursorProperties(properties -> {
            properties.put("first", "1");
            return properties;
        })).succeedsWithin(5, TimeUnit.SECONDS);

        assertThat(cursor.getCursorProperties()).containsEntry("first", "1");
    }

    @Test
    public void testUpdateCursorPropertiesCanRemoveEverything() throws Exception {
        assertThat(cursor.putCursorProperty("a", "1")).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(cursor.putCursorProperty("b", "2")).succeedsWithin(5, TimeUnit.SECONDS);

        assertThat(cursor.updateCursorProperties(properties -> {
            properties.clear();
            return properties;
        })).succeedsWithin(5, TimeUnit.SECONDS);

        assertThat(cursor.getCursorProperties()).isEmpty();
    }

    @Test
    public void testUpdateCursorPropertiesPropagatesFunctionFailureWithoutSideEffects() throws Exception {
        assertThat(cursor.putCursorProperty("k", "a")).succeedsWithin(5, TimeUnit.SECONDS);

        RuntimeException failure = new RuntimeException("boom");
        assertThat(cursor.updateCursorProperties(properties -> {
            properties.put("leak", "1");
            throw failure;
        })).failsWithin(5, TimeUnit.SECONDS)
                .withThrowableThat().havingCause().isSameAs(failure);

        assertThat(cursor.getCursorProperties()).containsEntry("k", "a").doesNotContainKey("leak");
    }

    @Test
    public void testUpdateCursorPropertiesRejectsNullReturn() throws Exception {
        assertThat(cursor.putCursorProperty("k", "a")).succeedsWithin(5, TimeUnit.SECONDS);

        assertThat(cursor.updateCursorProperties(properties -> null))
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableThat().havingCause().isInstanceOf(NullPointerException.class);

        assertThat(cursor.getCursorProperties()).containsEntry("k", "a");
        // The store write was skipped, so the version is untouched and a later update succeeds
        // without a version conflict.
        assertThat(cursor.putCursorProperty("k", "b")).succeedsWithin(5, TimeUnit.SECONDS);
    }

    @Test
    public void testUpdateCursorPropertiesSurvivesReopen() throws Exception {
        assertThat(cursor.updateCursorProperties(properties -> {
            properties.put("durable", "1");
            return properties;
        })).succeedsWithin(5, TimeUnit.SECONDS);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedger ledger2 = factory2.open("update-cursor-properties-test-ledger");
        ManagedCursor reopenedCursor = ledger2.openCursor("c1");

        assertThat(reopenedCursor.getCursorProperties()).containsEntry("durable", "1");
    }

    @Test(timeOut = 60_000)
    public void testConcurrentUpdateCursorPropertiesBothSurviveRetry() throws Exception {
        // Two closures racing on the metadata version: both retry BadVersion, so both must
        // succeed and the final state must contain both edits.
        for (int i = 0; i < 100; i++) {
            assertThat(cursor.updateCursorProperties(properties -> {
                properties.remove("left");
                properties.remove("right");
                return properties;
            })).succeedsWithin(5, TimeUnit.SECONDS);

            CompletableFuture<Void> left = flattened(() -> Futures.executeWithRetry(
                    () -> cursor.updateCursorProperties(properties -> {
                        properties.put("left", "1");
                        return properties;
                    }), ManagedLedgerException.BadVersionException.class, 50));
            CompletableFuture<Void> right = flattened(() -> Futures.executeWithRetry(
                    () -> cursor.updateCursorProperties(properties -> {
                        properties.put("right", "1");
                        return properties;
                    }), ManagedLedgerException.BadVersionException.class, 50));

            assertThat(left).succeedsWithin(20, TimeUnit.SECONDS);
            assertThat(right).succeedsWithin(20, TimeUnit.SECONDS);
            assertThat(cursor.getCursorProperties()).as("iteration " + i)
                    .containsEntry("left", "1")
                    .containsEntry("right", "1");
        }
    }

    @Test(timeOut = 60_000)
    public void testConcurrentReplacementSurvivesConditionalRemoval() throws Exception {
        // A putCursorProperty(k, b) overlapping a conditional removal of (k, a) must leave the
        // property at b once the put has been applied, whichever way the removal interleaves.
        // The removal either succeeds or fails with BadVersion; both preserve the invariant. The
        // put races on the metadata version, so like the broker callers it retries until it
        // succeeds.
        for (int i = 0; i < 100; i++) {
            assertThat(Futures.executeWithRetry(() -> cursor.putCursorProperty("k", "a"),
                    ManagedLedgerException.BadVersionException.class, 50))
                    .succeedsWithin(10, TimeUnit.SECONDS);

            CompletableFuture<Void> removal = flattened(() ->
                    cursor.updateCursorProperties(conditionalRemoval("k", "a")));
            CompletableFuture<Void> replacement = flattened(() ->
                    Futures.executeWithRetry(() -> cursor.putCursorProperty("k", "b"),
                            ManagedLedgerException.BadVersionException.class, 50));

            CompletableFuture<Void> removalSettled = removal.handle((__, ex) -> {
                if (ex == null) {
                    return null;
                }
                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                if (cause instanceof ManagedLedgerException.BadVersionException) {
                    return null;
                }
                throw new CompletionException(cause);
            });
            assertThat(replacement).succeedsWithin(20, TimeUnit.SECONDS);
            assertThat(removalSettled).succeedsWithin(20, TimeUnit.SECONDS);
            assertThat(cursor.getCursorProperties()).as("iteration " + i).containsEntry("k", "b");
        }
    }

    @Test
    public void testConcurrentNonDurableUpdatesKeepBothEdits() throws Exception {
        // A non-durable cursor has no store version guarding the write, so the update runs the
        // read, the function and the publication as one critical section. Without it, two
        // concurrent updates would both succeed and only the last edit would survive.
        ManagedCursor nonDurable = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        for (int i = 0; i < 10; i++) {
            nonDurable.updateCursorProperties(properties -> {
                properties.clear();
                return properties;
            }).get(5, TimeUnit.SECONDS);

            CompletableFuture<Void> left = flattened(() -> nonDurable.updateCursorProperties(properties -> {
                try {
                    // Hold the read snapshot open so the right update reads the same state.
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                properties.put("left", "1");
                return properties;
            }));
            Thread.sleep(10);
            CompletableFuture<Void> right = flattened(() -> nonDurable.updateCursorProperties(properties -> {
                properties.put("right", "1");
                return properties;
            }));

            assertThat(left).succeedsWithin(5, TimeUnit.SECONDS);
            assertThat(right).succeedsWithin(5, TimeUnit.SECONDS);
            assertThat(nonDurable.getCursorProperties()).as("iteration " + i)
                    .containsEntry("left", "1")
                    .containsEntry("right", "1");
        }
    }

    @Test
    public void testDefaultUpdateCursorPropertiesAppliesPerKeyDifferences() {
        // The interface default is a best-effort fallback: it applies the per-key differences
        // through putCursorProperty/removeCursorProperty, and a throwing function fails the
        // returned future instead of throwing synchronously.
        Map<String, String> properties = new java.util.concurrent.ConcurrentHashMap<>();
        ManagedCursor cursorWithDefault = Mockito.mock(ManagedCursor.class, CALLS_REAL_METHODS);
        Mockito.doAnswer(invocation -> {
            properties.put(invocation.getArgument(0), invocation.getArgument(1));
            return CompletableFuture.completedFuture(null);
        }).when(cursorWithDefault).putCursorProperty(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(invocation -> {
            properties.remove(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        }).when(cursorWithDefault).removeCursorProperty(Mockito.anyString());
        Mockito.doReturn(properties).when(cursorWithDefault).getCursorProperties();

        assertThat(cursorWithDefault.updateCursorProperties(current -> {
            current.put("added", "2");
            current.put("keep", "3");
            current.remove("drop");
            return current;
        })).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(properties)
                .containsEntry("added", "2")
                .containsEntry("keep", "3")
                .doesNotContainKey("drop");

        RuntimeException failure = new RuntimeException("boom");
        assertThat(cursorWithDefault.updateCursorProperties(current -> {
            throw failure;
        })).failsWithin(5, TimeUnit.SECONDS)
                .withThrowableThat().havingCause().isSameAs(failure);

        // A concurrent writer changes the live map after the baseline copy: the diff must use the
        // baseline, so a closure that returns its input unchanged issues no write instead of
        // overwriting the writer with the stale value.
        properties.put("K", "A");
        clearInvocations(cursorWithDefault);
        assertThat(cursorWithDefault.updateCursorProperties(current -> {
            properties.put("K", "B");
            return current;
        })).succeedsWithin(5, TimeUnit.SECONDS);
        verify(cursorWithDefault, never()).putCursorProperty(Mockito.anyString(), Mockito.anyString());
        verify(cursorWithDefault, never()).removeCursorProperty(Mockito.anyString());

        // A null property map behaves as an empty one instead of throwing synchronously.
        Mockito.doReturn(null).when(cursorWithDefault).getCursorProperties();
        assertThat(cursorWithDefault.updateCursorProperties(current -> current))
                .succeedsWithin(5, TimeUnit.SECONDS);
    }

    @Test
    public void testMarkDeleteVersionRefreshAdoptsStoredProperties() throws Exception {
        // The mark-delete persist fails with BadVersion after another factory's cursor wrote the
        // metadata. The ownership refresh must adopt the stored properties together with the
        // fresh version: leaving the stale properties paired with the fresh version would let an
        // update computed from the stale properties win the store's version check.
        CountDownLatch openLatch = new CountDownLatch(1);
        AtomicReference<ManagedLedger> openedLedger = new AtomicReference<>();
        factory.asyncOpen("version-refresh-test-ledger", new ManagedLedgerConfig(),
                new AsyncCallbacks.OpenLedgerCallback() {
                    @Override
                    public void openLedgerComplete(ManagedLedger managedLedger, Object ctx) {
                        openedLedger.set(managedLedger);
                        openLatch.countDown();
                    }

                    @Override
                    public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        openLatch.countDown();
                    }
                }, () -> CompletableFuture.completedFuture(true), null);
        assertThat(openLatch.await(10, TimeUnit.SECONDS)).isTrue();
        ManagedLedger ownedLedger = openedLedger.get();
        ManagedCursor ownedCursor = ownedLedger.openCursor("c1");
        assertThat(ownedCursor.putCursorProperty("k", "a")).succeedsWithin(5, TimeUnit.SECONDS);
        Position position = ownedLedger.addEntry("entry".getBytes(StandardCharsets.UTF_8));

        @Cleanup("shutdown")
        ManagedLedgerFactory externalFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedCursor externalCursor = externalFactory.open("version-refresh-test-ledger").openCursor("c1");
        assertThat(externalCursor.putCursorProperty("k", "b")).succeedsWithin(5, TimeUnit.SECONDS);

        CompletableFuture<Void> markDelete = new CompletableFuture<>();
        ownedCursor.asyncMarkDelete(position, new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                markDelete.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                markDelete.completeExceptionally(exception);
            }
        }, null);
        // The refresh fails the top-level call so the next mark-delete retries, so settling with
        // BadVersion is expected either way.
        CompletableFuture<Void> markDeleteSettled = markDelete.handle((res, ex) -> {
            if (ex == null) {
                return null;
            }
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            if (cause instanceof ManagedLedgerException.BadVersionException) {
                return null;
            }
            throw new CompletionException(cause);
        });
        assertThat(markDeleteSettled).succeedsWithin(10, TimeUnit.SECONDS);
        assertThat(ownedCursor.getCursorProperties()).containsEntry("k", "b");
    }

    private static CompletableFuture<Void> flattened(
            Supplier<CompletableFuture<Void>> operation) {
        return CompletableFuture.supplyAsync(operation).thenCompose(future -> future);
    }
}
