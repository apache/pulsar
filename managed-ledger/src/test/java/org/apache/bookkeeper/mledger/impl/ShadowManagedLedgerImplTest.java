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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@CustomLog
public class ShadowManagedLedgerImplTest extends MockedBookKeeperTestCase {

    private ShadowManagedLedgerImpl openShadowManagedLedger(String name, String sourceName)
            throws ManagedLedgerException, InterruptedException {
        return openShadowManagedLedger(name, sourceName, defaultConfig());
    }

    private ShadowManagedLedgerImpl openShadowManagedLedger(String name, String sourceName, ManagedLedgerConfig config)
            throws ManagedLedgerException, InterruptedException {
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
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open("source_ML", defaultConfig()
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
            // LCE is not updated.
            log.info().attr("sourceLCE", sourceML.lastConfirmedEntry)
                    .attr("shadowLCE", shadowML.lastConfirmedEntry)
                    .log("Last confirmed entries after write to old ledger");
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
            Awaitility.await().untilAsserted(() -> {
                assertEquals(shadowML.ledgers.size(), 5);
                assertEquals(shadowML.currentLedgerEntries, 0);
            });
            assertEquals(future.get(), fakePos);
            // LCE should be updated.
            log.info().attr("sourceLCE", sourceML.lastConfirmedEntry)
                    .attr("shadowLCE", shadowML.lastConfirmedEntry)
                    .log("Last confirmed entries after write to future ledger");
            assertEquals(sourceML.lastConfirmedEntry, shadowML.lastConfirmedEntry);
        }
    }

    private ManagedLedgerImpl openSourceManagedLedgerWithClosedLedgers(String name) throws Exception {
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open(name, defaultConfig()
                .setMaxEntriesPerLedger(2)
                .setRetentionTime(-1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1));
        byte[] data = new byte[10];
        for (int i = 0; i < 5; i++) {
            sourceML.addEntry(data);
        }
        assertEquals(sourceML.ledgers.size(), 3);
        return sourceML;
    }

    private static Set<Long> ledgerIds(ManagedLedgerImpl ml) {
        return ml.ledgers.values().stream().map(LedgerInfo::getLedgerId).collect(Collectors.toSet());
    }

    private void assertLedgersRemainInBookKeeper(Set<Long> ledgerIds) {
        Awaitility.await().during(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertTrue(bkc.getLedgers().containsAll(ledgerIds),
                        "Ledgers " + ledgerIds + " should remain in " + bkc.getLedgers()));
    }

    @Test
    public void testShadowTrimmingKeepsSourceLedgers() throws Exception {
        ManagedLedgerImpl sourceML = openSourceManagedLedgerWithClosedLedgers("source_ML_trim");
        Set<Long> sourceLedgerIds = ledgerIds(sourceML);

        // The shadow uses the default zero retention and has no cursors, so its closed ledgers are trimmable.
        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML_trim", "source_ML_trim");
        assertEquals(ledgerIds(shadowML), sourceLedgerIds);

        CompletableFuture<Void> promise = new CompletableFuture<>();
        shadowML.trimConsumedLedgersInBackground(promise);
        promise.get(10, TimeUnit.SECONDS);
        assertEquals(shadowML.ledgers.size(), 1);

        assertLedgersRemainInBookKeeper(sourceLedgerIds);
        assertEquals(ledgerIds(sourceML), sourceLedgerIds);
    }

    @Test
    public void testShadowDeletionKeepsSourceLedgers() throws Exception {
        ManagedLedgerImpl sourceML = openSourceManagedLedgerWithClosedLedgers("source_ML_delete");
        Set<Long> sourceLedgerIds = ledgerIds(sourceML);

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML_delete", "source_ML_delete");
        assertEquals(ledgerIds(shadowML), sourceLedgerIds);

        shadowML.delete();

        assertLedgersRemainInBookKeeper(sourceLedgerIds);
        assertEquals(ledgerIds(sourceML), sourceLedgerIds);
    }

    @Test
    public void testClosedShadowDeletionKeepsSourceLedgers() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open("source_ML_closed_delete", defaultConfig()
                .setMaxEntriesPerLedger(2)
                .setRetentionTime(-1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1)
                .setLedgerOffloader(offloader));
        byte[] data = new byte[10];
        for (int i = 0; i < 5; i++) {
            sourceML.addEntry(data);
        }
        Set<Long> sourceLedgerIds = ledgerIds(sourceML);
        sourceML.offloadPrefix(sourceML.getLastConfirmedEntry());
        Set<Long> offloadedLedgerIds = Set.copyOf(offloader.offloadedLedgers());
        assertEquals(offloadedLedgerIds.size(), 2);

        ManagedLedgerConfig shadowConfig = defaultConfig().setLedgerOffloader(offloader);
        ShadowManagedLedgerImpl shadowML =
                openShadowManagedLedger("shadow_ML_closed_delete", "source_ML_closed_delete", shadowConfig);
        assertEquals(ledgerIds(shadowML), sourceLedgerIds);
        shadowML.close();

        // The shadow is no longer open, so the factory deletes it based on its stored metadata.
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
        factory.asyncDelete("shadow_ML_closed_delete", CompletableFuture.completedFuture(shadowConfig),
                new AsyncCallbacks.DeleteLedgerCallback() {
                    @Override
                    public void deleteLedgerComplete(Object ctx) {
                        deleteFuture.complete(null);
                    }

                    @Override
                    public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        deleteFuture.completeExceptionally(exception);
                    }
                }, null);
        deleteFuture.get(10, TimeUnit.SECONDS);
        assertThrows(ManagedLedgerException.class, () -> factory.getManagedLedgerInfo("shadow_ML_closed_delete"));

        assertLedgersRemainInBookKeeper(sourceLedgerIds);
        assertEquals(offloader.offloadedLedgers(), offloadedLedgerIds);
        assertTrue(offloader.deletedOffloads().isEmpty(), "Deleted offloads: " + offloader.deletedOffloads());
        assertEquals(ledgerIds(sourceML), sourceLedgerIds);
    }

    @Test
    public void testShadowReopenedWithoutSourceKeepsSourceLedgers() throws Exception {
        ManagedLedgerImpl sourceML = openSourceManagedLedgerWithClosedLedgers("source_ML_reopen");
        Set<Long> sourceLedgerIds = ledgerIds(sourceML);

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML_reopen", "source_ML_reopen");
        assertEquals(ledgerIds(shadowML), sourceLedgerIds);
        shadowML.close();

        // Open the stored shadow metadata without a shadow source, as happens when the source property is no
        // longer part of the topic properties.
        ManagedLedgerImpl reopenedML = (ManagedLedgerImpl) factory.open("shadow_ML_reopen", defaultConfig());
        assertFalse(reopenedML instanceof ShadowManagedLedgerImpl);
        assertTrue(reopenedML.getProperties().containsKey(ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY));

        CompletableFuture<Void> promise = new CompletableFuture<>();
        reopenedML.trimConsumedLedgersInBackground(promise);
        promise.get(10, TimeUnit.SECONDS);
        assertLedgersRemainInBookKeeper(sourceLedgerIds);

        reopenedML.delete();
        assertLedgersRemainInBookKeeper(sourceLedgerIds);
        assertEquals(ledgerIds(sourceML), sourceLedgerIds);
    }

    @Test
    public void testShadowSourcePropertyCannotBeRemoved() throws Exception {
        openSourceManagedLedgerWithClosedLedgers("source_ML_property");
        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML_property", "source_ML_property");
        assertThrows(ManagedLedgerException.class,
                () -> shadowML.deleteProperty(ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY));
        assertTrue(shadowML.getProperties().containsKey(ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY));
    }

    @Test
    public void testShadowOffloadKeepsSourceOffloadedData() throws Exception {
        // The offload of the source never completes, so its ledger metadata keeps an offload id that is in use.
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader() {
            @Override
            public CompletableFuture<Void> offload(ReadHandle ledger, UUID uuid, Map<String, String> extraMetadata) {
                offloads.putIfAbsent(ledger.getId(), uuid);
                return new CompletableFuture<>();
            }
        };
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open("source_ML_offload", defaultConfig()
                .setMaxEntriesPerLedger(2)
                .setRetentionTime(-1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1)
                .setLedgerOffloader(offloader));
        byte[] data = new byte[10];
        for (int i = 0; i < 5; i++) {
            sourceML.addEntry(data);
        }
        long firstLedgerId = sourceML.ledgers.firstKey();
        sourceML.asyncOffloadPrefix(sourceML.getLastConfirmedEntry(), new AsyncCallbacks.OffloadCallback() {
            @Override
            public void offloadComplete(Position pos, Object ctx) {
            }

            @Override
            public void offloadFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(offloader.offloadedLedgers().contains(firstLedgerId));
            assertTrue(sourceML.ledgers.get(firstLedgerId).getOffloadContext().hasUidMsb());
        });
        UUID sourceOffloadId = offloader.offloads.get(firstLedgerId);

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML_offload", "source_ML_offload",
                defaultConfig().setLedgerOffloader(offloader));
        assertTrue(shadowML.ledgers.get(firstLedgerId).getOffloadContext().hasUidMsb());

        CompletableFuture<Position> shadowOffload = new CompletableFuture<>();
        shadowML.asyncOffloadPrefix(shadowML.getLastConfirmedEntry(), new AsyncCallbacks.OffloadCallback() {
            @Override
            public void offloadComplete(Position pos, Object ctx) {
                shadowOffload.complete(pos);
            }

            @Override
            public void offloadFailed(ManagedLedgerException exception, Object ctx) {
                shadowOffload.completeExceptionally(exception);
            }
        }, null);

        Awaitility.await().during(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(offloader.deletedOffloads().isEmpty(), "Deleted offloads: " + offloader.deletedOffloads());
            assertEquals(offloader.offloads.get(firstLedgerId), sourceOffloadId);
        });
        assertTrue(shadowOffload.isCompletedExceptionally());
    }

    @Test
    public void testShadowAutomaticOffloadAndOffloadLagTrimmingKeepSourceData() throws Exception {
        // The source offloads its first ledger and keeps the BookKeeper copy of it.
        OffloadPrefixTest.MockLedgerOffloader sourceOffloader = new OffloadPrefixTest.MockLedgerOffloader();
        sourceOffloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(TimeUnit.HOURS.toMillis(1));
        ManagedLedgerImpl sourceML = (ManagedLedgerImpl) factory.open("source_ML_auto_offload", defaultConfig()
                .setMaxEntriesPerLedger(2)
                .setRetentionTime(-1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1)
                .setLedgerOffloader(sourceOffloader));
        byte[] data = new byte[10];
        for (int i = 0; i < 5; i++) {
            sourceML.addEntry(data);
        }
        Set<Long> sourceLedgerIds = ledgerIds(sourceML);
        long firstLedgerId = sourceML.ledgers.firstKey();
        long secondLedgerId = sourceML.ledgers.higherKey(firstLedgerId);
        sourceML.offloadPrefix(PositionFactory.create(secondLedgerId, 0));
        assertTrue(sourceML.ledgers.get(firstLedgerId).getOffloadContext().isComplete());
        assertEquals(sourceOffloader.offloadedLedgers(), Set.of(firstLedgerId));
        UUID sourceOffloadId = sourceOffloader.offloads.get(firstLedgerId);

        // The shadow offloads everything automatically and removes offloaded ledgers from BookKeeper at once.
        // Its clock is ahead of the offload timestamp of the source, so the deletion lag has already passed.
        OffloadPrefixTest.MockLedgerOffloader shadowOffloader = new OffloadPrefixTest.MockLedgerOffloader();
        shadowOffloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(0L);
        shadowOffloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(0L);
        Clock shadowClock = Clock.offset(Clock.systemUTC(), Duration.ofMinutes(1));
        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML_auto_offload",
                "source_ML_auto_offload", defaultConfig()
                        .setRetentionTime(-1, TimeUnit.DAYS)
                        .setRetentionSizeInMB(-1)
                        .setLedgerOffloader(shadowOffloader)
                        .setClock(shadowClock));
        assertEquals(ledgerIds(shadowML), sourceLedgerIds);
        assertTrue(shadowML.ledgers.get(firstLedgerId).getOffloadContext().isComplete());

        shadowML.maybeOffloadInBackground(ManagedLedgerImpl.AUTOMATIC_OFFLOAD_TRIGGER);
        awaitExecutorTasks(shadowML);
        CompletableFuture<Void> trimPromise = new CompletableFuture<>();
        shadowML.trimConsumedLedgersInBackground(trimPromise);
        trimPromise.get(10, TimeUnit.SECONDS);
        shadowML.maybeOffloadInBackground(ManagedLedgerImpl.AUTOMATIC_OFFLOAD_TRIGGER);
        awaitExecutorTasks(shadowML);

        assertLedgersRemainInBookKeeper(sourceLedgerIds);
        assertTrue(shadowOffloader.offloadedLedgers().isEmpty(),
                "Offloaded by the shadow: " + shadowOffloader.offloadedLedgers());
        assertTrue(shadowOffloader.deletedOffloads().isEmpty(),
                "Deleted by the shadow: " + shadowOffloader.deletedOffloads());
        assertTrue(sourceOffloader.deletedOffloads().isEmpty(),
                "Deleted offloads: " + sourceOffloader.deletedOffloads());
        assertEquals(sourceOffloader.offloads.get(firstLedgerId), sourceOffloadId);
        assertEquals(ledgerIds(sourceML), sourceLedgerIds);
        assertTrue(sourceML.ledgers.get(firstLedgerId).getOffloadContext().isComplete());
        assertFalse(sourceML.ledgers.get(firstLedgerId).getOffloadContext().isBookkeeperDeleted());
    }

    /**
     * Waits until the tasks submitted so far to the executor of the managed ledger have run. Offload requests of
     * a managed ledger run on its executor, so this waits for them to be processed.
     */
    private static void awaitExecutorTasks(ManagedLedgerImpl ml) throws Exception {
        CompletableFuture<Void> barrier = new CompletableFuture<>();
        ml.executor.execute(() -> barrier.complete(null));
        barrier.get(10, TimeUnit.SECONDS);
    }

    private String storedShadowSource(String name) throws Exception {
        Map<String, String> properties = factory.getManagedLedgerInfo(name).properties;
        return properties == null ? null : properties.get(ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY);
    }

    private void deleteThroughFactory(String name, ManagedLedgerConfig config) throws Exception {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
        factory.asyncDelete(name, CompletableFuture.completedFuture(config),
                new AsyncCallbacks.DeleteLedgerCallback() {
                    @Override
                    public void deleteLedgerComplete(Object ctx) {
                        deleteFuture.complete(null);
                    }

                    @Override
                    public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        deleteFuture.completeExceptionally(exception);
                    }
                }, null);
        deleteFuture.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testExistingManagedLedgerOpenedAsShadowStoresSourceProperty() throws Exception {
        ManagedLedgerImpl sourceML = openSourceManagedLedgerWithClosedLedgers("source_ML_converted");
        Set<Long> sourceLedgerIds = ledgerIds(sourceML);

        // An existing managed ledger is created without properties, as for a partition of a partitioned topic
        // whose shadow source is later set in the partitioned topic metadata.
        ManagedLedger existingML = factory.open("shadow_ML_converted", defaultConfig());
        existingML.close();
        assertEquals(storedShadowSource("shadow_ML_converted"), null);

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML_converted", "source_ML_converted");
        assertEquals(ledgerIds(shadowML), sourceLedgerIds);
        shadowML.close();

        assertEquals(storedShadowSource("shadow_ML_converted"), "source_topic");

        // Deleting it without the shadow source in the config keeps the ledgers of the source.
        deleteThroughFactory("shadow_ML_converted", null);
        assertLedgersRemainInBookKeeper(sourceLedgerIds);
        assertEquals(ledgerIds(sourceML), sourceLedgerIds);
    }

    @Test
    public void testShadowWithoutStoredSourcePropertyDeletionKeepsSourceLedgers() throws Exception {
        ManagedLedgerImpl sourceML = openSourceManagedLedgerWithClosedLedgers("source_ML_unmarked");
        Set<Long> sourceLedgerIds = ledgerIds(sourceML);

        ShadowManagedLedgerImpl shadowML = openShadowManagedLedger("shadow_ML_unmarked", "source_ML_unmarked");
        assertEquals(ledgerIds(shadowML), sourceLedgerIds);
        shadowML.close();

        // Store the metadata as written by earlier versions: the source ledgers without the shadow source property.
        MetaStore store = factory.getMetaStore();
        CompletableFuture<Void> updateFuture = new CompletableFuture<>();
        store.getManagedLedgerInfo("shadow_ML_unmarked", false, new MetaStore.MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                ManagedLedgerInfo unmarkedInfo = new ManagedLedgerInfo();
                unmarkedInfo.addAllLedgerInfos(mlInfo.getLedgerInfosList());
                store.asyncUpdateLedgerIds("shadow_ML_unmarked", unmarkedInfo, stat,
                        new MetaStore.MetaStoreCallback<>() {
                            @Override
                            public void operationComplete(Void result, Stat stat) {
                                updateFuture.complete(null);
                            }

                            @Override
                            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                                updateFuture.completeExceptionally(e);
                            }
                        });
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                updateFuture.completeExceptionally(e);
            }
        });
        updateFuture.get(10, TimeUnit.SECONDS);
        assertEquals(storedShadowSource("shadow_ML_unmarked"), null);
        assertEquals(factory.getManagedLedgerInfo("shadow_ML_unmarked").ledgers.stream()
                .map(li -> li.ledgerId).collect(Collectors.toSet()), sourceLedgerIds);

        // The shadow source is supplied through the config used for the deletion.
        ManagedLedgerConfig deleteConfig = defaultConfig();
        deleteConfig.setProperties(Map.of(ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY, "source_topic"));
        deleteThroughFactory("shadow_ML_unmarked", deleteConfig);
        assertThrows(ManagedLedgerException.class, () -> factory.getManagedLedgerInfo("shadow_ML_unmarked"));

        assertLedgersRemainInBookKeeper(sourceLedgerIds);
        assertEquals(ledgerIds(sourceML), sourceLedgerIds);
    }
}
