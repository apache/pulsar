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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerBatchReadTest extends MockedBookKeeperTestCase {
    @DataProvider
    public Object[][] readPaths() {
        return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "readPaths")
    public void testBatchReadWithLeadingEmptyEntry(boolean reopen, boolean cacheEnabled) throws Exception {
        factory.getEntryCacheManager().updateCacheSizeAndThreshold(cacheEnabled ? 1024 * 1024 : 0);
        ManagedLedgerConfig config = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1);
        config.setPulsarMessageEntries(false);
        config.setBatchReadEnabled(true);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("batch-read", config);
        ManagedCursor cursor = ledger.openCursor("cursor");
        byte[][] payloads = {new byte[0], new byte[2048], new byte[]{42}};
        for (byte[] payload : payloads) {
            ledger.addEntry(payload);
        }
        // Keep the ledger's average entry size small enough that the cursor requests all three entries.
        // The batch size limit must still split the unexpectedly large second entry into its own round.
        for (int i = 0; i < 8; i++) {
            ledger.addEntry(new byte[]{1});
        }
        if (reopen) {
            ledger.close();
            ledger = (ManagedLedgerImpl) factory.open("batch-read", config);
            cursor = ledger.openCursor("cursor");
        }
        factory.getEntryCacheManager().clear();
        List<List<Long>> rounds = new ArrayList<>();
        bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
            rounds.add(List.of(first, last));
            return CompletableFuture.completedFuture(entries);
        });
        try {
            CompletableFuture<List<Entry>> result = new CompletableFuture<>();
            cursor.asyncReadEntries(3, 1024, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    result.complete(entries);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    result.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);
            List<Entry> entries = result.get(10, TimeUnit.SECONDS);
            try {
                assertThat(entries).hasSize(3);
                for (int i = 0; i < payloads.length; i++) {
                    assertThat(entries.get(i).getData()).isEqualTo(payloads[i]);
                }
                assertThat(rounds).containsExactly(List.of(0L, 0L), List.of(1L, 1L), List.of(2L, 2L));
            } finally {
                entries.forEach(Entry::release);
            }
        } finally {
            bkc.setReadHandleInterceptor(null);
            ledger.close();
        }
    }
}
