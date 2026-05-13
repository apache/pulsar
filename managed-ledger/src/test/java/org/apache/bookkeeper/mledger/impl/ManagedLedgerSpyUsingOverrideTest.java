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
import static org.testng.Assert.assertEquals;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.awaitility.Awaitility;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class ManagedLedgerSpyUsingOverrideTest {

    private static final Charset Encoding = StandardCharsets.UTF_8;

    @Factory
    public Object[] createNestedTestInstances() {
        return new Object[]{new AdvanceCursorsIfNecessaryThrowsExceptionTest()};
    }

    static class AdvanceCursorsIfNecessaryThrowsExceptionTest extends MockedBookKeeperTestCase {

        private AtomicInteger advanceCursorsIfNecessaryCallTimes;

        @Override
        protected ManagedLedgerFactoryImpl initManagedLedgerFactory(MetadataStoreExtended metadataStore,
                                                                BookKeeper bookKeeper,
                                                                ManagedLedgerFactoryConfig managedLedgerFactoryConfig,
                                                                ManagedLedgerConfig managedLedgerConfig)
                throws Exception {
            return new ManagedLedgerFactoryImpl(metadataStore, bookKeeper, managedLedgerFactoryConfig,
                    managedLedgerConfig) {
                @Override
                protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                                                            ManagedLedgerConfig config,
                                                            Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
                    return new ManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name, mlOwnershipChecker) {
                        @Override
                        void advanceCursorsIfNecessary(List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgersToDelete)
                                throws ManagedLedgerException.LedgerNotExistException {
                            advanceCursorsIfNecessaryCallTimes.incrementAndGet();
                            throw new ManagedLedgerException.LedgerNotExistException(
                                    "First non deleted Ledger is not found");
                        }
                    };
                }
            };
        }

        @Override
        protected void setUpTestCase() throws Exception {
            advanceCursorsIfNecessaryCallTimes = new AtomicInteger(0);
            super.setUpTestCase();
        }

        @Override
        protected void cleanUpTestCase() throws Exception {
            advanceCursorsIfNecessaryCallTimes.set(0);
            super.cleanUpTestCase();
        }

        @Test(invocationCount = 1000)
        public void testLockReleaseWhenTrimLedger() throws Exception {
            ManagedLedgerConfig config = new ManagedLedgerConfig();
            initManagedLedgerConfig(config);
            config.setMaxEntriesPerLedger(1);

            ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testLockReleaseWhenTrimLedger", config);
            final int entries = 10;
            ManagedCursor cursor = ledger.openCursor("test-cursor" + UUID.randomUUID());
            for (int i = 0; i < entries; i++) {
                ledger.addEntry(String.valueOf(i).getBytes(Encoding));
            }

            // Wait for new ledger created to avoid flaky test, so currentLedger is the last emptyLedger.
            // This make sure that we will read entries by calling ReadHandle instead of using entry cache.
            // If we don't wait for new ledger created, we may lose the last valid ReadHandle(emptyLedgerLedgerId-1)
            // in ledgerCache.
            Awaitility.await().untilAsserted(() -> {
                assertEquals(ledger.ledgers.size() - 1, entries);
            });

            List<Entry> entryList = cursor.readEntries(entries);
            assertEquals(entryList.size(), entries);

            // We don't need to wait here since read operation is completed.
            assertEquals(ledger.ledgerCache.size() - 1, entries - 1);

            cursor.clearBacklog();
            ledger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
            ledger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);

            // Cleanup fails because ManagedLedgerNotFoundException is thrown
            assertEquals(ledger.ledgers.size() - 1, entries);
            assertEquals(ledger.ledgerCache.size() - 1, entries - 1);

            // The lock is released even if an ManagedLedgerNotFoundException occurs, so it can be called repeatedly
            Awaitility.await().untilAsserted(() -> assertThat(advanceCursorsIfNecessaryCallTimes.get()).isEqualTo(3));
            cursor.close();
            ledger.close();
        }
    }

}
