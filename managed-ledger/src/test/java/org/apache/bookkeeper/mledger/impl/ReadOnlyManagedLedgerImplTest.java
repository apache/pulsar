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
import static org.testng.Assert.assertEquals;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ReadOnlyManagedLedgerImplTest extends MockedBookKeeperTestCase {
    private static final String MANAGED_LEDGER_NAME_NON_PROPERTIES = "ml-non-properties";
    private static final String MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES = "ml-attached-properties";


    @Override
    protected void initManagedLedgerFactoryConfig(ManagedLedgerFactoryConfig config) {
        super.initManagedLedgerFactoryConfig(config);
        // A same-name cache can retain the writer's ledger and hide a direct read-only storage failure.
        config.setMaxCacheSize(0);
    }

    @Test
    public void testDirectReadWithoutCursorFromLiveLedger() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger writer = factory.open("direct-live-ledger", config);
        Position first = writer.addEntry("first".getBytes(StandardCharsets.UTF_8));
        Position penultimate = writer.addEntry("penultimate".getBytes(StandardCharsets.UTF_8));
        Position last = writer.addEntry("last".getBytes(StandardCharsets.UTF_8));

        ReadOnlyManagedLedger reader = openReadOnly("direct-live-ledger", config).get(10, TimeUnit.SECONDS);
        assertThat(reader.getNumberOfEntries()).isEqualTo(3);
        assertDirectEntry(reader, first, "first");
        assertDirectEntry(reader, penultimate, "penultimate");
        assertDirectEntry(reader, last, "last");
        assertThat(reader.getNumberOfEntries()).isEqualTo(3);
    }

    @Test
    public void testDirectReadWithoutCursorFromClosedLedger() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS)
                .setRetentionSizeInMB(-1).setMaxEntriesPerLedger(2);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedger writer = factory.open("direct-closed-ledger", config);
        Position first = writer.addEntry("first".getBytes(StandardCharsets.UTF_8));
        Position lastClosed = writer.addEntry("last-closed".getBytes(StandardCharsets.UTF_8));
        Position current = writer.addEntry("current".getBytes(StandardCharsets.UTF_8));
        assertThat(current.getLedgerId()).isNotEqualTo(first.getLedgerId());

        ReadOnlyManagedLedger reader = openReadOnly("direct-closed-ledger", config).get(10, TimeUnit.SECONDS);
        assertDirectEntry(reader, first, "first");
        assertDirectEntry(reader, lastClosed, "last-closed");
        assertDirectEntry(reader, current, "current");
        assertThat(reader.getNumberOfEntries()).isEqualTo(3);
    }

    @Test
    public void testDirectReadWithoutCursorWithEmptyLastLedger() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(-1);
        ManagedLedger writer = factory.open("direct-empty-last-ledger", config);
        Position published = writer.addEntry("published".getBytes(StandardCharsets.UTF_8));
        writer.close();
        ManagedLedger reopened = factory.open("direct-empty-last-ledger", config);
        var metadata = factory.getManagedLedgerInfo("direct-empty-last-ledger");
        var emptyLedger = metadata.ledgers.get(metadata.ledgers.size() - 1);
        assertThat(emptyLedger.ledgerId).isGreaterThan(published.getLedgerId());
        assertThat(emptyLedger.entries).isNull();
        assertThat(reopened.getLastConfirmedEntry()).isEqualTo(published);

        ReadOnlyManagedLedger reader = openReadOnly("direct-empty-last-ledger", config).get(10, TimeUnit.SECONDS);
        assertDirectEntry(reader, published, "published");
        assertThat(reader.getNumberOfEntries()).isEqualTo(1);
        Position absent = PositionFactory.create(emptyLedger.ledgerId, 0);
        assertThatThrownBy(() -> {
            Entry unexpected = directRead(reader, absent).get(10, TimeUnit.SECONDS);
            unexpected.release();
        })
                .hasCauseInstanceOf(ManagedLedgerException.class)
                .satisfies(error -> assertThat(error.getCause().getMessage())
                        .doesNotContain("LastConfirmedEntry is null"));
    }

    @Test
    public void testEmptyReadOnlyLedgerWithoutCursor() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        factory.open("direct-empty-ledger", config);
        ReadOnlyManagedLedger reader = openReadOnly("direct-empty-ledger", config).get(10, TimeUnit.SECONDS);
        assertThat(reader.getNumberOfEntries()).isZero();
    }

    @Test
    public void testDirectReadOnlyOpenMissingLedger() {
        assertThatThrownBy(() -> openReadOnly("direct-missing-ledger", defaultConfig()).get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerNotFoundException.class);
    }

    @Test
    public void testDirectReadOnlyOpenFailure() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger writer = factory.open("direct-open-failure", config);
        writer.addEntry("published".getBytes(StandardCharsets.UTF_8));
        bkc.failNow(BKException.Code.ReadException);
        assertThatThrownBy(() -> openReadOnly("direct-open-failure", config).get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.class);
    }

    private CompletableFuture<ReadOnlyManagedLedger> openReadOnly(String name, ManagedLedgerConfig config) {
        CompletableFuture<ReadOnlyManagedLedger> result = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(name, new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger ledger, Object ctx) {
                result.complete(ledger);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException error, Object ctx) {
                result.completeExceptionally(error);
            }
        }, config, null);
        return result;
    }

    private CompletableFuture<Entry> directRead(ReadOnlyManagedLedger ledger, Position position) {
        CompletableFuture<Entry> result = new CompletableFuture<>();
        ledger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                result.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException error, Object ctx) {
                result.completeExceptionally(error);
            }
        }, null);
        return result;
    }

    private void assertDirectEntry(ReadOnlyManagedLedger ledger, Position position, String expected) throws Exception {
        Entry entry = directRead(ledger, position).get(10, TimeUnit.SECONDS);
        try {
            assertThat(entry.getPosition()).isEqualTo(position);
            assertThat(entry.getData()).isEqualTo(expected.getBytes(StandardCharsets.UTF_8));
        } finally {
            entry.release();
        }
    }

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

}
