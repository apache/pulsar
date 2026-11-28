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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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

        ReadOnlyManagedLedger reader = openReadOnly("direct-live-ledger", config);
        try {
            assertThat(reader.getNumberOfEntries()).isEqualTo(3);
            assertDirectEntry(reader, first, "first");
            assertDirectEntry(reader, penultimate, "penultimate");
            assertDirectEntry(reader, last, "last");
            assertThat(reader.getNumberOfEntries()).isEqualTo(3);
        } finally {
            reader.asyncClose().get(10, TimeUnit.SECONDS);
        }
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

        ReadOnlyManagedLedger reader = openReadOnly("direct-closed-ledger", config);
        try {
            assertDirectEntry(reader, first, "first");
            assertDirectEntry(reader, lastClosed, "last-closed");
            assertDirectEntry(reader, current, "current");
            assertThat(reader.getNumberOfEntries()).isEqualTo(3);
        } finally {
            reader.asyncClose().get(10, TimeUnit.SECONDS);
        }
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

        ReadOnlyManagedLedger reader = openReadOnly("direct-empty-last-ledger", config);
        try {
            assertDirectEntry(reader, published, "published");
            assertThat(reader.getNumberOfEntries()).isEqualTo(1);
            Position absent = PositionFactory.create(emptyLedger.ledgerId, 0);
            assertThatThrownBy(() -> directRead(reader, absent).release())
                    .hasCauseInstanceOf(ManagedLedgerException.class)
                    .satisfies(error -> assertThat(error.getCause().getMessage())
                            .doesNotContain("LastConfirmedEntry is null"));
        } finally {
            reader.asyncClose().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testEmptyReadOnlyLedgerWithoutCursor() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        factory.open("direct-empty-ledger", config);
        ReadOnlyManagedLedger reader = openReadOnly("direct-empty-ledger", config);
        try {
            assertThat(reader.getNumberOfEntries()).isZero();
        } finally {
            reader.asyncClose().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDirectReadOnlyOpenMissingLedger() {
        assertThatThrownBy(() -> openReadOnly("direct-missing-ledger", defaultConfig()))
                .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerNotFoundException.class);
    }

    @Test
    public void testDirectReadOnlyOpenFailure() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger writer = factory.open("direct-open-failure", config);
        writer.addEntry("published".getBytes(StandardCharsets.UTF_8));
        bkc.failNow(BKException.Code.ReadException);
        assertThatThrownBy(() -> openReadOnly("direct-open-failure", config))
                .hasCauseInstanceOf(ManagedLedgerException.class);
    }

    private ReadOnlyManagedLedger openReadOnly(String name, ManagedLedgerConfig config) throws Exception {
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
        try {
            return result.get(10, TimeUnit.SECONDS);
        } catch (Exception error) {
            result.thenAccept(ReadOnlyManagedLedger::asyncClose);
            throw error;
        }
    }

    private Entry directRead(ReadOnlyManagedLedger ledger, Position position) throws Exception {
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
        try {
            return result.get(10, TimeUnit.SECONDS);
        } catch (Exception error) {
            result.thenAccept(Entry::release);
            throw error;
        }
    }

    private void assertDirectEntry(ReadOnlyManagedLedger ledger, Position position, String expected) throws Exception {
        Entry entry = directRead(ledger, position);
        try {
            assertThat(entry.getPosition()).isEqualTo(position);
            assertThat(entry.getData()).isEqualTo(expected.getBytes(StandardCharsets.UTF_8));
        } finally {
            entry.release();
        }
    }

    @Test
    public void testReadOnlyManagedLedgerImplAttachProperties() throws Exception {
        final ManagedLedger ledger = factory.open(MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES,
                defaultConfig().setRetentionTime(1, TimeUnit.HOURS));
        final String propertiesKey = "test-key";
        final String propertiesValue = "test-value";

        ledger.setConfig(defaultConfig());
        ledger.addEntry("entry-0".getBytes());
        Map<String, String> properties = Map.of(propertiesKey, propertiesValue);
        ledger.setProperties(properties);
        ReadOnlyManagedLedger reader = openReadOnly(MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES, defaultConfig());
        try {
            assertThat(reader.getProperties()).containsExactlyEntriesOf(properties);
        } finally {
            reader.asyncClose().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadOnlyManagedLedgerImplNoProperties() throws Exception {
        final ManagedLedger ledger = factory.open(MANAGED_LEDGER_NAME_NON_PROPERTIES,
                defaultConfig().setRetentionTime(1, TimeUnit.HOURS));
        ledger.setConfig(defaultConfig());
        ledger.addEntry("entry-0".getBytes());
        ReadOnlyManagedLedger reader = openReadOnly(MANAGED_LEDGER_NAME_NON_PROPERTIES, defaultConfig());
        try {
            assertThat(reader.getProperties()).isEmpty();
        } finally {
            reader.asyncClose().get(10, TimeUnit.SECONDS);
        }
    }

}
