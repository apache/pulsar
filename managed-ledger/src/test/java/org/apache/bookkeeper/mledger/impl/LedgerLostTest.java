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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class LedgerLostTest extends MockedBookKeeperTestCase {

    private Position[][] makeManyLedgers(ManagedLedger ledger, int ledgerCount, int maxEntriesPerLedger)
            throws Exception {
        Position[][] allPos = new Position[ledgerCount][];
        for (int i = 0; i < ledgerCount; i++){
            Position[] posInSameLedger = new Position[maxEntriesPerLedger];
            allPos[i] = posInSameLedger;
            for (int j = 0; j < maxEntriesPerLedger; j++){
                posInSameLedger[j] = ledger.addEntry(String.format("%s_%s", i, j).getBytes(Charset.defaultCharset()));
            }
            ledger.rollCurrentLedgerIfFull();
        }
        return allPos;
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

    @Test
    public void testConcurrentTrimLedgerAndOpenNewCursor2() throws Exception {
        String managedLedgerName = "lg_" + UUID.randomUUID().toString().replaceAll("-","_");
        String cursorName1 = "cs_01";
        String cursorName2 = "cs_02";
        int maxEntriesPerLedger = 5;
        int ledgerCount = 5;
        ManagedLedgerImpl.LOCK.set(100);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setThrottleMarkDelete(1);
        config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
        config.setMaxEntriesPerLedger(5);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(managedLedgerName, config);
        ManagedCursorImpl cursor1 = (ManagedCursorImpl) ledger.openCursor(cursorName1);

        Position[][] allPos = makeManyLedgers(ledger, ledgerCount, maxEntriesPerLedger);
        List<Position> deletePositions = new ArrayList<>();
        deletePositions.addAll(Arrays.asList(allPos[0]));
        deletePositions.addAll(Arrays.asList(allPos[1]));
        deletePositions.addAll(Arrays.asList(allPos[2]));
        cursor1.delete(allPos[0][0]);
        cursor1.delete(deletePositions);

        Thread.sleep(5 * 1000);

        ManagedLedgerImpl.LOCK.set(0);

        ledger.maybeUpdateCursorBeforeTrimmingConsumedLedger();
        CompletableFuture trimLedgerFuture = new CompletableFuture();
        CompletableFuture<ManagedCursorImpl> cursorFuture = new CompletableFuture();
        new Thread(() -> {
            openCursor(ledger, cursorName2, cursorFuture);
        }).start();
        ledger.trimConsumedLedgersInBackground(trimLedgerFuture);
        trimLedgerFuture.join();
        ManagedCursorImpl cursor2 = cursorFuture.join();

        long deletedLedger =  allPos[0][0].getLedgerId();
        Assert.assertTrue(ledger.getLedgerInfo(deletedLedger).join() == null);
        Assert.assertEquals(cursor2.getReadPosition().getLedgerId(), deletedLedger);
        // cleanup.
        ledger.close();
    }
}
