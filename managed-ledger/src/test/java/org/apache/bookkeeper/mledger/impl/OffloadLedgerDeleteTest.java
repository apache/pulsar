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

import static org.apache.bookkeeper.mledger.impl.OffloadPrefixTest.assertEventuallyTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.util.MockClock;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OffloadLedgerDeleteTest extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(OffloadLedgerDeleteTest.class);

    @Test
    public void testLaggedDelete() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(new Long(300000));
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        int i = 0;
        for (; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise);
        promise.join();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise2 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise2);
        promise2.join();

        // assert bk ledger is deleted
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));

        // ledger still exists in list
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());

        // move past retention, should be deleted from offloaded also
        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise3 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise3);
        promise3.join();

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedgerId));
    }

    @Test
    public void testLaggedDeleteRetentionSetLower() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(5, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(new Long(600000));
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        int i = 0;
        for (; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise);
        promise.join();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise2 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise2);
        promise2.join();

        // ensure it gets deleted from both bookkeeper and offloader
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));
        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedgerId));
    }

    @Test
    public void testLaggedDeleteSlowConsumer() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(new Long(300000));
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("sub1");

        for (int i = 0; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);

        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise);
        promise.join();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise2 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise2);
        promise2.join();

        // assert bk ledger is deleted
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));

        // ledger still exists in list
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
    }
}
