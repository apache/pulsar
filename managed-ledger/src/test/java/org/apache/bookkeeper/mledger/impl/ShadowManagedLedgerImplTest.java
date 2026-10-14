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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@CustomLog
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
    public void testShadowOpenCountsExistingSourceEntriesWithoutNotifications() throws Exception {
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open("existing_source_ML", new ManagedLedgerConfig()
                .setMaxEntriesPerLedger(10)
                .setRetentionTime(-1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1));
        byte[] data = new byte[10];
        Position first = sourceML.addEntry(data);
        Position second = sourceML.addEntry(data);
        assertEquals(first.getLedgerId(), second.getLedgerId());

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("existing_shadow_ML", "existing_source_ML");
        assertEquals(shadowML.getLastConfirmedEntry(), second);
        assertEquals(shadowML.currentLedgerEntries, 2L);
        assertEquals(shadowML.getPositionAfterN(first, 1, PositionBound.startExcluded), second);
    }

    @Test
    public void testShadowOpenAfterSourceRolloverCountsCurrentLedgerEntries() throws Exception {
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open("rollover_source_ML", new ManagedLedgerConfig()
                .setMaxEntriesPerLedger(3)
                .setRetentionTime(-1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1));
        byte[] data = new byte[10];
        Position oldLedgerPosition = sourceML.addEntry(data);
        sourceML.addEntry(data);
        sourceML.addEntry(data);
        Position firstInCurrentLedger = sourceML.addEntry(data);
        Position secondInCurrentLedger = sourceML.addEntry(data);
        assertTrue(firstInCurrentLedger.getLedgerId() > oldLedgerPosition.getLedgerId());
        assertEquals(firstInCurrentLedger.getLedgerId(), secondInCurrentLedger.getLedgerId());

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("rollover_shadow_ML", "rollover_source_ML");
        assertEquals(shadowML.getLastConfirmedEntry(), secondInCurrentLedger);
        assertEquals(shadowML.currentLedgerEntries, 2L);
        assertEquals(shadowML.getPositionAfterN(firstInCurrentLedger, 1, PositionBound.startExcluded),
                secondInCurrentLedger);
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
            log.info().attr("position", pos).log("Added entry");
            positions.add(pos);
        }
        log.info().attr("currentLedgerId", sourceML.currentLedger.getId()).log("Current ledger");
        assertEquals(sourceML.ledgers.size(), 3);

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML", "source_ML");
        //After init, the state should be the same.
        assertEquals(shadowML.ledgers.size(), 3);
        assertEquals(sourceML.currentLedger.getId(), shadowML.currentLedger.getId());
        assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);

        //Add new data to source ML
        Position newPos = sourceML.addEntry(data);

        //Add new data to source ML, and a new ledger rolled
        Awaitility.await().untilAsserted(() -> {
            assertEquals(sourceML.ledgers.size(), 4);
            assertEquals(shadowML.ledgers.size(), 4);
            assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        });
        log.info().attr("sourceLCE", sourceML.lastConfirmedEntry)
                .attr("shadowLCE", shadowML.lastConfirmedEntry).log("Last confirmed entries");

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
            // A retry from an older ledger must not hide entries already visible in the shadow topic.
            log.info().attr("sourceLCE", sourceML.lastConfirmedEntry)
                    .attr("shadowLCE", shadowML.lastConfirmedEntry)
                    .log("Last confirmed entries after write to old ledger");
            assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
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
            log.info().attr("sourceLCE", sourceML.lastConfirmedEntry)
                    .attr("shadowLCE", shadowML.lastConfirmedEntry)
                    .log("Last confirmed entries after write to current ledger");
            assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        }

        {// test write entry with ledgerId > currentLedger
            Position fakePos = PositionFactory.create(newPos.getLedgerId() + 1, newPos.getEntryId());

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

            sourceML.addEntry(data); // new ledger rolled.
            sourceML.addEntry(data);
            Awaitility.await().untilAsserted(() -> assertEquals(shadowML.ledgers.size(), 5));
            assertEquals(future.get(), fakePos);
            assertEquals(shadowML.currentLedgerEntries, fakePos.getEntryId() + 1);
            // LCE should be updated.
            log.info().attr("sourceLCE", sourceML.lastConfirmedEntry)
                    .attr("shadowLCE", shadowML.lastConfirmedEntry)
                    .log("Last confirmed entries after write to future ledger");
            assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        }
    }

    @Test
    public void testShadowWriteRetryDoesNotMoveLastConfirmedEntryBackwards() throws Exception {
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open("retry_source_ML", new ManagedLedgerConfig()
                .setMaxEntriesPerLedger(4)
                .setRetentionTime(-1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1));
        byte[] data = new byte[10];
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            positions.add(sourceML.addEntry(data));
        }
        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("retry_shadow_ML", "retry_source_ML");
        Position lastConfirmed = positions.get(5);
        assertEquals(shadowML.getLastConfirmedEntry(), lastConfirmed);
        assertTrue(positions.get(1).getLedgerId() < lastConfirmed.getLedgerId());
        assertEquals(positions.get(4).getLedgerId(), lastConfirmed.getLedgerId());

        // Both older-ledger and current-ledger retries retain the original callback position while
        // leaving the readable range unchanged. Marker notifications use the same write path.
        for (Position retryPosition : List.of(positions.get(1), positions.get(4), lastConfirmed)) {
            assertEquals(writeShadowEntry(shadowML, data, retryPosition), retryPosition);
            assertEquals(shadowML.getLastConfirmedEntry(), lastConfirmed);
            if (retryPosition.equals(positions.get(4))) {
                assertEquals(shadowML.getPositionAfterN(retryPosition, 1, PositionBound.startExcluded),
                        lastConfirmed, "An old notification must not make the current ledger appear empty");
            }
        }

        Position newPosition = sourceML.addEntry(data);
        assertEquals(newPosition.getLedgerId(), lastConfirmed.getLedgerId());
        assertEquals(shadowML.getLastConfirmedEntry(), lastConfirmed);
        assertEquals(writeShadowEntry(shadowML, data, newPosition), newPosition);
        assertEquals(shadowML.getLastConfirmedEntry(), newPosition);

        sourceML.addEntry(data);
        Position positionAfterRollover = sourceML.addEntry(data);
        assertTrue(positionAfterRollover.getLedgerId() > newPosition.getLedgerId());
        assertEquals(writeShadowEntry(shadowML, data, positionAfterRollover), positionAfterRollover);
        assertEquals(shadowML.getLastConfirmedEntry(), positionAfterRollover);
        assertEquals(writeShadowEntry(shadowML, data, newPosition), newPosition);
        assertEquals(shadowML.getLastConfirmedEntry(), positionAfterRollover);
        assertEquals(shadowML.currentLedgerEntries, positionAfterRollover.getEntryId() + 1);
    }

    private Position writeShadowEntry(ShadowManagedLedgerImpl shadowML, byte[] data, Position position)
            throws Exception {
        CompletableFuture<Position> future = new CompletableFuture<>();
        shadowML.asyncAddEntry(data, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position persistedPosition, ByteBuf entryData, Object ctx) {
                future.complete(persistedPosition);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, position);
        return future.get(10, TimeUnit.SECONDS);
    }
}
