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

import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class LedgerLostTest extends MockedBookKeeperTestCase {

    private void triggerLedgerRollover(ManagedLedger ledger, int maxEntriesPerLedger) {
        new Thread(() -> {
            int writeLedgerCount = 2;
            for (int i = 0; i < writeLedgerCount; i++) {
                for (int j = 0; j < maxEntriesPerLedger; j++) {
                    byte[] data = String.format("%s_%s", i, j).getBytes(Charset.defaultCharset());
                    Object ctx = "";
                    ledger.asyncAddEntry(data, new AsyncCallbacks.AddEntryCallback() {
                        @Override
                        public void addComplete(Position position, ByteBuf entryData, Object ctx) {

                        }

                        @Override
                        public void addFailed(ManagedLedgerException exception, Object ctx) {

                        }
                    }, ctx);
                }
            }
        }).start();
    }

    private CompletableFuture<ManagedCursorImpl> openCursor(ManagedLedger ledger, String cursorName,
                                                            CompletableFuture<ManagedCursorImpl> cursorFuture){
        ledger.asyncOpenCursor(cursorName, CommandSubscribe.InitialPosition.Earliest,
                new AsyncCallbacks.OpenCursorCallback(){
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        cursorFuture.complete((ManagedCursorImpl) cursor);
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        cursorFuture.completeExceptionally(exception);
                    }
                }, cursorName);
        return cursorFuture;
    }

    private int calculateCursorCount(ManagedLedgerImpl ledger){
        Iterator iterator = ledger.getCursors().iterator();
        int count = 0;
        while (iterator.hasNext()){
            iterator.next();
            count++;
        }
        return count;
    }

    @Test
    public void testConcurrentCloseLedgerAndSwitchLedger() throws Exception {
        String managedLedgerName = "lg_" + UUID.randomUUID().toString().replaceAll("-", "_");
        String cursorName1 = "cs_01";
        String cursorName2 = "cs_02";
        int maxEntriesPerLedger = 5;
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setThrottleMarkDelete(1);
        config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
        config.setMaxEntriesPerLedger(5);

        // call "switch ledger" and "managedLedger.close" concurrently.
        ManagedLedgerImpl.PROCESS_COODINATOR.set(100);
        final ManagedLedgerImpl managedLedger1 = (ManagedLedgerImpl) factory.open(managedLedgerName, config);
        ManagedLedgerImpl.PROCESS_COODINATOR.set(0);
        triggerLedgerRollover(managedLedger1, maxEntriesPerLedger);
        managedLedger1.close();

        // 1. close managedLedger1.
        // 2. create new ManagedLedger: managedLedger2.
        //   2-1: create cursor1 of managedLedger2.
        // 3. close managedLedger1 twice, make managedLedger2 remove from cache of managedLedgerFactory.
        // 4. create new ManagedLedger: managedLedger3.
        //   4-2: create cursor2 of managedLedger3.
        // Then two ManagedLedger appear at the same time and have different numbers of cursors.
        int expectCursorNumInManagedLedger2 = 1;
        int expectCursorNumInManagedLedger3 = 2;
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(managedLedger1.getState() != ManagedLedgerImpl.State.Closed));
        final ManagedLedgerImpl managedLedger2 = (ManagedLedgerImpl) factory.open(managedLedgerName, config);
        Awaitility.await().until(() -> {
            CompletableFuture<ManagedLedgerImpl> managedLedgerFuture = factory.ledgers.get(managedLedgerName);
            return managedLedgerFuture.join() == managedLedger2;
        });
        managedLedger1.close();
        managedLedger2.openCursor(cursorName1, CommandSubscribe.InitialPosition.Earliest);
        Assert.assertEquals(managedLedger1.getState(), ManagedLedgerImpl.State.Closed);
        Assert.assertTrue(factory.ledgers.isEmpty());
        final ManagedLedgerImpl managedLedger3 = (ManagedLedgerImpl) factory.open(managedLedgerName, config);
        managedLedger3.openCursor(cursorName2, CommandSubscribe.InitialPosition.Earliest);
        Assert.assertTrue(managedLedger2.getState() != ManagedLedgerImpl.State.Closed);
        Assert.assertTrue(managedLedger3.getState() != ManagedLedgerImpl.State.Closed);
        Assert.assertEquals(calculateCursorCount(managedLedger2), expectCursorNumInManagedLedger2);
        Assert.assertEquals(calculateCursorCount(managedLedger3), expectCursorNumInManagedLedger3);
        System.out.println(1);
    }
}
