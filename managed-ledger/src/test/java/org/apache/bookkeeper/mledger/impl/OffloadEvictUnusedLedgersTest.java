/**
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class OffloadEvictUnusedLedgersTest extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(OffloadEvictUnusedLedgersTest.class);

    @Test
    public void testEvictUnusedLedgers() throws Exception {
        OffloadPrefixReadTest.MockLedgerOffloader offloader =
                new OffloadPrefixReadTest.MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        int inactiveOffloadedLedgerEvictionTimeMs = 10000;
        config.setInactiveOffloadedLedgerEvictionTimeMs(inactiveOffloadedLedgerEvictionTimeMs, TimeUnit.MILLISECONDS);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger_evict", config);

        // no evict when no offloaded ledgers
        assertTrue(ledger.internalEvictOffloadedLedgers().isEmpty());

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());

        // ledgers should be marked as offloaded
        ledger.getLedgersInfoAsList().stream().allMatch(l -> l.hasOffloadContext());

        // no evict when no offloaded ledgers are marked as inactive
        assertTrue(ledger.internalEvictOffloadedLedgers().isEmpty());

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        int j = 0;
        for (Entry e : cursor.readEntries(25)) {
            assertEquals(new String(e.getData()), "entry-" + j++);
        }
        cursor.close();

        // set last access time to be 2x inactiveOffloadedLedgerEvictionTimeMs
        AtomicLong first = new AtomicLong(-1);
        assertTrue(!ledger.ledgerCache.isEmpty());
        ledger.ledgerCache.forEach((id, l) -> {
            if (first.compareAndSet(-1, id)) {
                OffloadPrefixReadTest.MockOffloadReadHandle handle =
                        (OffloadPrefixReadTest.MockOffloadReadHandle) l.join();
                handle.setLastAccessTimestamp(System.currentTimeMillis() - inactiveOffloadedLedgerEvictionTimeMs * 2);
            }
        });
        assertNotEquals(first.get(), -1L);

        List<Long> evicted = ledger.internalEvictOffloadedLedgers();
        assertEquals(evicted.size(), 1);
        assertEquals(first.get(), evicted.get(0).longValue());

    }

}
