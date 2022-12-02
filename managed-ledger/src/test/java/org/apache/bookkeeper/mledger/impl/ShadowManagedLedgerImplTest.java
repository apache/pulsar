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

import static org.testng.Assert.*;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
public class ShadowManagedLedgerImplTest extends MockedBookKeeperTestCase {

    private ShadowManagedLedgerImpl openShadowManagedLedger(String name, String sourceName)
            throws ManagedLedgerException, InterruptedException {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setShadowSourceName(sourceName);
        Map<String, String> properties = new HashMap<>();
        properties.put(ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY, "source_topic");
        config.setProperties(properties);
        ManagedLedger shadowML = factory.open(name, config);
        assertTrue(shadowML instanceof ShadowManagedLedgerImpl);
        return (ShadowManagedLedgerImpl) shadowML;
    }

    @Test
    public void testShadowWrites() throws Exception {
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open("source_ML", new ManagedLedgerConfig()
                .setMaxEntriesPerLedger(2)
                .setRetentionTime(-1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1));
        byte[] data = new byte[10];
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Position pos = sourceML.addEntry(data);
            log.info("pos={}", pos);
            positions.add(pos);
        }
        log.info("currentLedgerId:{}", sourceML.currentLedger.getId());
        assertEquals(sourceML.ledgers.size(), 3);

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML", "source_ML");
        //After init, the state should be the same.
        assertEquals(shadowML.ledgers.size(), 3);
        assertEquals(sourceML.currentLedger.getId(), shadowML.currentLedger.getId());
        assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);

        //Add new data to source ML
        Position newPos = sourceML.addEntry(data);

        // The state should not be the same.
        log.info("Source.LCE={},Shadow.LCE={}", sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        assertNotEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);

        //Add new data to source ML, and a new ledger rolled
        newPos = sourceML.addEntry(data);
        assertEquals(sourceML.ledgers.size(), 4);
        Awaitility.await().untilAsserted(()->assertEquals(shadowML.ledgers.size(), 4));
        log.info("Source.LCE={},Shadow.LCE={}", sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        Awaitility.await().untilAsserted(()->assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry));

        {// test write entry with ledgerId < currentLedger
            CompletableFuture<Position> future = new CompletableFuture<>();
            shadowML.asyncAddEntry(data, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    future.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, positions.get(2));
            assertEquals(future.get(), positions.get(2));
            // LCE is not updated.
            log.info("1.Source.LCE={},Shadow.LCE={}", sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
            assertNotEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        }

        {// test write entry with ledgerId == currentLedger
            newPos = sourceML.addEntry(data);
            assertEquals(sourceML.ledgers.size(), 4);
            assertNotEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);

            CompletableFuture<Position> future = new CompletableFuture<>();
            shadowML.asyncAddEntry(data, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    future.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, newPos);
            assertEquals(future.get(), newPos);
            // LCE should be updated.
            log.info("2.Source.LCE={},Shadow.LCE={}", sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
            assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        }

        {// test write entry with ledgerId > currentLedger
            PositionImpl fakePos = PositionImpl.get(newPos.getLedgerId() + 1, newPos.getEntryId());

            CompletableFuture<Position> future = new CompletableFuture<>();
            shadowML.asyncAddEntry(data, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    future.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, fakePos);
            //This write will be queued unit new ledger is rolled in source.

            newPos = sourceML.addEntry(data); // new ledger rolled.
            newPos = sourceML.addEntry(data);
            Awaitility.await().untilAsserted(() -> assertEquals(shadowML.ledgers.size(), 5));
            assertEquals(future.get(), fakePos);
            // LCE should be updated.
            log.info("3.Source.LCE={},Shadow.LCE={}", sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
            assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        }
    }
}