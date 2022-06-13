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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.base.Charsets;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedTrash;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ManagedTrashTest extends MockedBookKeeperTestCase {

    private static final Charset Encoding = Charsets.UTF_8;

    private List<Long> deletedLedgers = new ArrayList<>();

    PulsarMockBookKeeper bkc;

    FaultInjectionMetadataStore metadataStore;

    private Map<String, byte[]> persistedData = new HashMap<>();


    @Override
    protected void setUpTestCase() throws Exception {
        metadataStore = new FaultInjectionMetadataStore(
                MetadataStoreExtended.create("memory:local", MetadataStoreConfig.builder().build())) {
            @Override
            public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
                CompletableFuture<Stat> future = super.put(path, value, expectedVersion);
                future.whenComplete((res, e) -> {
                    if (e != null) {
                        return;
                    }
                    persistedData.put(path, value);
                });
                return future;
            }

        };
        bkc = new PulsarMockBookKeeper(executor) {
            @Override
            public void asyncDeleteLedger(long lId, AsyncCallback.DeleteCallback cb, Object ctx) {
                getProgrammedFailure().thenComposeAsync((res) -> {
                    if (lId >= 10000) {
                        throw new IllegalArgumentException("LedgerId is invalid");
                    }
                    if (ledgers.containsKey(lId)) {
                        ledgers.remove(lId);
                        deletedLedgers.add(lId);
                        return FutureUtils.value(null);
                    } else {
                        return FutureUtils.exception(new BKException.BKNoSuchLedgerExistsException());
                    }
                }, executor).whenCompleteAsync((res, exception) -> {
                    if (exception != null) {
                        cb.deleteComplete(getExceptionCode(exception), ctx);
                    } else {
                        cb.deleteComplete(BKException.Code.OK, ctx);
                    }
                }, executor);
            }
        };
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
    }

    @Override
    protected void cleanUpTestCase() throws Exception {
        super.cleanUpTestCase();
        deletedLedgers.clear();
        persistedData.clear();
    }

    private LedgerInfo buildOffloadLedgerInfo(long ledgerId, UUID uuid) {
        LedgerInfo.Builder builder = LedgerInfo.newBuilder().setLedgerId(ledgerId);
        builder.getOffloadContextBuilder()
                .setUidMsb(uuid.getMostSignificantBits())
                .setUidLsb(uuid.getLeastSignificantBits());
        return builder.build();
    }

    @Test
    public void testTrashKeyOrder() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setDeleteIntervalSeconds(1);
        managedLedgerConfig.setArchiveDataLimitSize(10);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);

        managedTrash.appendLedgerTrashData(10, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(6, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(100, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(3, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(7, null, ManagedTrash.LedgerType.LEDGER);


        UUID uuid = UUID.randomUUID();
        LedgerInfo ledgerInfo = buildOffloadLedgerInfo(7, uuid);
        managedTrash.appendLedgerTrashData(7, ledgerInfo, ManagedTrash.LedgerType.OFFLOAD_LEDGER);

        Field field1 = ManagedTrashImpl.class.getDeclaredField("trashData");
        field1.setAccessible(true);
        ConcurrentSkipListMap map = (ConcurrentSkipListMap) field1.get(managedTrash);
        System.out.println(map);

        Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry = map.pollFirstEntry();
        Assert.assertEquals(entry.getKey().getLedgerId(), 3);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        entry = map.pollFirstEntry();
        Assert.assertEquals(entry.getKey().getLedgerId(), 6);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        if (uuid.getMostSignificantBits() > 0) {
            entry = map.pollFirstEntry();
            Assert.assertEquals(entry.getKey().getLedgerId(), 7);
            Assert.assertEquals(entry.getKey().getMsb(), 0);
            Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

            entry = map.pollFirstEntry();
            Assert.assertEquals(entry.getKey().getLedgerId(), 7);
            Assert.assertEquals(entry.getKey().getMsb(), uuid.getMostSignificantBits());
            Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.OFFLOAD_LEDGER);
        } else {
            entry = map.pollFirstEntry();
            Assert.assertEquals(entry.getKey().getLedgerId(), 7);
            Assert.assertEquals(entry.getKey().getMsb(), uuid.getMostSignificantBits());
            Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.OFFLOAD_LEDGER);

            entry = map.pollFirstEntry();
            Assert.assertEquals(entry.getKey().getLedgerId(), 7);
            Assert.assertEquals(entry.getKey().getMsb(), 0);
            Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);
        }

        entry = map.pollFirstEntry();
        Assert.assertEquals(entry.getKey().getLedgerId(), 10);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        entry = map.pollFirstEntry();
        Assert.assertEquals(entry.getKey().getLedgerId(), 100);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);
    }


    @Test
    public void testGetToDeleteDataFilterByManagedLedgers() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setMaxEntriesPerLedger(10);
        managedLedgerConfig.setRetentionTime(3, TimeUnit.SECONDS);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);

        Field field1 = ManagedTrashImpl.class.getDeclaredField("managedLedgers");
        field1.setAccessible(true);

        NavigableMap<Long, LedgerInfo> managedLedgers = (NavigableMap<Long, LedgerInfo>) field1.get(managedTrash);

        LedgerInfo emptyLedgerInfo = LedgerInfo.newBuilder().setLedgerId(-1).build();
        for (int i = 0; i < 30; i++) {
            long ledgerId = 10000 + i;
            managedLedgers.put(ledgerId, emptyLedgerInfo);
            if (i % 2 == 0) {
                managedTrash.appendLedgerTrashData(ledgerId, null, ManagedTrash.LedgerType.LEDGER);
            } else {
                //build offload ledger
                UUID uuid = UUID.randomUUID();
                LedgerInfo ledgerInfo = buildOffloadLedgerInfo(ledgerId, uuid);
                managedTrash.appendLedgerTrashData(ledgerId, ledgerInfo, ManagedTrash.LedgerType.OFFLOAD_LEDGER);
            }
        }
        ManagedTrashImpl.Tuple tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.BOTH);
        assertEquals(tuple.getToDelete().size(), 0);
        assertEquals(tuple.isFiltered(), true);

        //when managedLedgers remove 29, it can be to delete.
        managedLedgers.remove(10029L);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.BOTH);
        assertEquals(tuple.getToDelete().size(), 1);
        assertEquals(tuple.isFiltered(), true);
        assertEquals(tuple.getToDelete().get(0).getKey().getLedgerId(), 10029L);
        assertEquals(tuple.getToDelete().get(0).getKey().getType(), ManagedTrash.LedgerType.OFFLOAD_LEDGER);


        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.LEDGER);
        assertEquals(tuple.getToDelete().size(), 0);
        assertEquals(tuple.isFiltered(), true);


        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.OFFLOAD_LEDGER);
        assertEquals(tuple.getToDelete().size(), 1);
        assertEquals(tuple.isFiltered(), true);
        assertEquals(tuple.getToDelete().get(0).getKey().getLedgerId(), 10029L);
        assertEquals(tuple.getToDelete().get(0).getKey().getType(), ManagedTrash.LedgerType.OFFLOAD_LEDGER);

        managedLedgers.remove(10028L);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.BOTH);
        assertEquals(tuple.getToDelete().size(), 2);
        assertEquals(tuple.isFiltered(), true);
        assertEquals(tuple.getToDelete().get(0).getKey().getLedgerId(), 10029L);
        assertEquals(tuple.getToDelete().get(0).getKey().getType(), ManagedTrash.LedgerType.OFFLOAD_LEDGER);
        assertEquals(tuple.getToDelete().get(1).getKey().getLedgerId(), 10028L);
        assertEquals(tuple.getToDelete().get(1).getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.LEDGER);
        assertEquals(tuple.getToDelete().size(), 1);
        assertEquals(tuple.isFiltered(), true);
        assertEquals(tuple.getToDelete().get(0).getKey().getLedgerId(), 10028L);
        assertEquals(tuple.getToDelete().get(0).getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.OFFLOAD_LEDGER);
        assertEquals(tuple.getToDelete().size(), 1);
        assertEquals(tuple.isFiltered(), true);
        assertEquals(tuple.getToDelete().get(0).getKey().getLedgerId(), 10029L);
        assertEquals(tuple.getToDelete().get(0).getKey().getType(), ManagedTrash.LedgerType.OFFLOAD_LEDGER);
    }

    @Test
    public void testGetToDeleteDataFilterByLastTimeStamp() throws Exception {
        int maxDeleteCount = 3;
        int deleteIntervalSeconds = 5;
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setMaxEntriesPerLedger(10);
        managedLedgerConfig.setRetentionTime(3, TimeUnit.SECONDS);
        managedLedgerConfig.setMaxDeleteCount(maxDeleteCount);
        managedLedgerConfig.setDeleteIntervalSeconds(deleteIntervalSeconds);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);

        Field field1 = ManagedTrashImpl.class.getDeclaredField("managedLedgers");
        field1.setAccessible(true);

        for (int i = 0; i < 30; i++) {
            long ledgerId = 10000 + i;
            if (i % 2 == 0) {
                managedTrash.appendLedgerTrashData(ledgerId, null, ManagedTrash.LedgerType.LEDGER);
            } else {
                //build offload ledger
                UUID uuid = UUID.randomUUID();
                LedgerInfo ledgerInfo = buildOffloadLedgerInfo(ledgerId, uuid);
                managedTrash.appendLedgerTrashData(ledgerId, ledgerInfo, ManagedTrash.LedgerType.OFFLOAD_LEDGER);
            }
        }
        ManagedTrashImpl.Tuple tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.BOTH);
        assertEquals(tuple.getToDelete().size(), 3);
        assertEquals(tuple.isFiltered(), false);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.LEDGER);
        assertEquals(tuple.getToDelete().size(), 3);
        assertEquals(tuple.isFiltered(), false);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.OFFLOAD_LEDGER);
        assertEquals(tuple.getToDelete().size(), 3);
        assertEquals(tuple.isFiltered(), false);

        for (int i = 0; i < 10; i++) {
            managedTrash.triggerDeleteInBackground();
        }


        Field field2 = ManagedTrashImpl.class.getDeclaredField("trashData");
        field2.setAccessible(true);
        ConcurrentSkipListMap<ManagedTrashImpl.TrashKey, LedgerInfo> trashData =
                (ConcurrentSkipListMap<ManagedTrashImpl.TrashKey, LedgerInfo>) field2.get(managedTrash);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(trashData.lastEntry().getKey().getRetryCount(), maxDeleteCount - 1);
        });


        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.BOTH);
        assertEquals(tuple.getToDelete().size(), 0);
        assertEquals(tuple.isFiltered(), true);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.LEDGER);
        assertEquals(tuple.getToDelete().size(), 0);
        assertEquals(tuple.isFiltered(), true);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.OFFLOAD_LEDGER);
        assertEquals(tuple.getToDelete().size(), 0);
        assertEquals(tuple.isFiltered(), true);

        Thread.sleep(deleteIntervalSeconds * 1000);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.BOTH);
        assertEquals(tuple.getToDelete().size(), 3);
        assertEquals(tuple.isFiltered(), false);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.LEDGER);
        assertEquals(tuple.getToDelete().size(), 3);
        assertEquals(tuple.isFiltered(), false);

        tuple = managedTrash.getToDeleteData(ManagedTrash.LedgerType.OFFLOAD_LEDGER);
        assertEquals(tuple.getToDelete().size(), 3);
        assertEquals(tuple.isFiltered(), false);
    }

    @Test
    public void testRecover() throws Exception {
        int entryCount = 30;
        ConcurrentSkipListMap<ManagedTrashImpl.TrashKey, LedgerInfo> map = new ConcurrentSkipListMap<>();
        LedgerInfo emptyLedgerInfo = LedgerInfo.newBuilder().setLedgerId(-1).build();
        for (int i = 0; i < entryCount; i++) {
            ManagedTrashImpl.TrashKey trashKey = new ManagedTrashImpl.TrashKey(3, i, 0, ManagedTrash.LedgerType.LEDGER);
            map.put(trashKey, emptyLedgerInfo);
        }
        metadataStore.put("/managed-trash/managed-ledger/my_test_ledger/delete", ManagedTrashImpl.serialize(map),
                Optional.of(-1L));
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setMaxEntriesPerLedger(10);
        managedLedgerConfig.setRetentionTime(1, TimeUnit.SECONDS);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);

        Field field1 = ManagedTrashImpl.class.getDeclaredField("trashData");
        field1.setAccessible(true);
        ConcurrentSkipListMap<ManagedTrashImpl.TrashKey, LedgerInfo> trashData =
                (ConcurrentSkipListMap<ManagedTrashImpl.TrashKey, LedgerInfo>) field1.get(managedTrash);

        assertEquals(trashData.size(), entryCount);

        for (int i = 0; i < entryCount; i++) {
            Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry1 = map.pollFirstEntry();
            Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry2 = trashData.pollFirstEntry();
            assertEquals(entry1.getKey().getLedgerId(), entry2.getKey().getLedgerId());
        }
    }


    @Test
    public void testManagedTrashDelete() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setMaxEntriesPerLedger(10);
        managedLedgerConfig.setRetentionTime(1, TimeUnit.SECONDS);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);
        for (int i = 0; i < 100; i++) {
            ledger.addEntry("test".getBytes(Encoding));
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(deletedLedgers.size(), 9);
        });
    }

    @Test
    public void testManagedTrashArchive() throws Exception {
        int entryCount = 30;
        int archiveLimit = 10;

        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setDeleteIntervalSeconds(1);
        managedLedgerConfig.setArchiveDataLimitSize(archiveLimit);
        managedLedgerConfig.setMaxDeleteCount(3);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);


        //the ledgerId >= 10000, it will delete failed. see line_142.
        for (int i = 0; i < entryCount; i++) {
            managedTrash.appendLedgerTrashData(10000 + i, null, ManagedTrash.LedgerType.LEDGER);
        }
        managedTrash.triggerDeleteInBackground();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(5, persistedData.size());
        });

        assertEquals(managedTrash.getTrashDataSize(), 0);

        Field field2 = ManagedTrashImpl.class.getDeclaredField("toArchiveCount");
        field2.setAccessible(true);
        AtomicInteger toArchiveCount = (AtomicInteger) field2.get(managedTrash);

        assertEquals(toArchiveCount.get(), 0);


        ConcurrentSkipListMap<ManagedTrashImpl.TrashKey, LedgerInfo> totalArchive = new ConcurrentSkipListMap<>();
        for (Map.Entry<String, byte[]> entry : persistedData.entrySet()) {
            if (entry.getKey().startsWith("/managed-trash")) {
                if (entry.getKey().endsWith("/delete")) {
                    byte[] value = entry.getValue();
                    assertEquals(value.length, 0);
                } else {
                    Map<ManagedTrashImpl.TrashKey, LedgerInfo> archiveData =
                            managedTrash.deSerialize(entry.getValue());
                    assertEquals(archiveData.size(), archiveLimit);
                    totalArchive.putAll(archiveData);
                }
            }
        }

        assertEquals(totalArchive.size(), entryCount);
        int index = 0;
        for (Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry : totalArchive.entrySet()) {
            assertEquals(entry.getKey().getLedgerId(), 10000 + index);
            index++;
        }

    }

    @Test
    public void testManagedTrashClose() throws Exception {
        //when managedTrash close, it will persist trashData.
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setDeleteIntervalSeconds(1);
        managedLedgerConfig.setArchiveDataLimitSize(10);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);

        managedTrash.appendLedgerTrashData(10, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(6, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(100, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(3, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(7, null, ManagedTrash.LedgerType.LEDGER);
        ledger.close();
        assertEquals(persistedData.size(), 2);

        byte[] content = persistedData.get("/managed-trash/managed-ledger/my_test_ledger/delete");
        NavigableMap<ManagedTrashImpl.TrashKey, LedgerInfo> persistedTrashData =
                managedTrash.deSerialize(content);

        assertEquals(persistedTrashData.size(), 5);

        Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry =
                persistedTrashData.pollFirstEntry();

        Assert.assertEquals(entry.getKey().getLedgerId(), 3);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        entry = persistedTrashData.pollFirstEntry();

        Assert.assertEquals(entry.getKey().getLedgerId(), 6);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        entry = persistedTrashData.pollFirstEntry();

        Assert.assertEquals(entry.getKey().getLedgerId(), 7);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        entry = persistedTrashData.pollFirstEntry();

        Assert.assertEquals(entry.getKey().getLedgerId(), 10);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        entry = persistedTrashData.pollFirstEntry();

        Assert.assertEquals(entry.getKey().getLedgerId(), 100);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);
    }

    @Test
    public void testGetAllArchiveIndex() throws Exception {
        int entryCount = 30;
        int archiveLimit = 10;

        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setDeleteIntervalSeconds(1);
        managedLedgerConfig.setArchiveDataLimitSize(archiveLimit);
        managedLedgerConfig.setMaxDeleteCount(3);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);


        //the ledgerId >= 10000, it will delete failed. see line_142.
        for (int i = 0; i < entryCount; i++) {
            managedTrash.appendLedgerTrashData(10000 + i, null, ManagedTrash.LedgerType.LEDGER);
        }
        managedTrash.triggerDeleteInBackground();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(5, persistedData.size());
        });

        CompletableFuture<List<Long>> index = managedTrash.getAllArchiveIndex();

        assertEquals(index.get().size(), 3);
    }

    @Test
    public void testGetArchiveData() throws Exception {
        int entryCount = 30;
        int archiveLimit = 10;

        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setDeleteIntervalSeconds(1);
        managedLedgerConfig.setArchiveDataLimitSize(archiveLimit);
        managedLedgerConfig.setMaxDeleteCount(3);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);


        //the ledgerId >= 10000, it will delete failed. see line_142.
        for (int i = 0; i < entryCount; i++) {
            managedTrash.appendLedgerTrashData(10000 + i, null, ManagedTrash.LedgerType.LEDGER);
        }
        managedTrash.triggerDeleteInBackground();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(5, persistedData.size());
        });

        CompletableFuture<List<Long>> indexFuture = managedTrash.getAllArchiveIndex();

        List<Long> indexes = indexFuture.get();

        Map<ManagedTrashImpl.TrashKey, LedgerInfo> trashedData = new ConcurrentSkipListMap<>();
        for (Long index : indexes) {
            trashedData.putAll(managedTrash.getArchiveData(index).get());
        }
        assertEquals(trashedData.size(), 30);

        int i = 0;
        for (Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry : trashedData.entrySet()) {
            Assert.assertEquals(entry.getKey().getRetryCount(), 0);
            Assert.assertEquals(entry.getKey().getLedgerId(), 10000 + i);
            Assert.assertEquals(entry.getKey().getMsb(), 0);
            Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);
            i++;
        }
    }

    @Test
    public void testAsyncCloseAfterAllTrashDataDeleteOnce() throws Exception {
        int entryCount = 30;
        int archiveLimit = 10;
        int maxDeleteCount = 3;

        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setDeleteIntervalSeconds(1);
        managedLedgerConfig.setArchiveDataLimitSize(archiveLimit);
        managedLedgerConfig.setMaxDeleteCount(maxDeleteCount);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        field.setAccessible(true);
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);


        //the ledgerId >= 10000, it will delete failed. see line_142.
        for (int i = 0; i < entryCount; i++) {
            managedTrash.appendLedgerTrashData(10000 + i, null, ManagedTrash.LedgerType.LEDGER);
        }

        try {
            managedTrash.asyncCloseAfterAllTrashDataDeleteOnce().get();
        } catch (Exception e) {
            Assert.fail();
        }
        assertEquals(managedTrash.state, ManagedTrashImpl.State.Closed);

        Field field1 = ManagedTrashImpl.class.getDeclaredField("trashData");
        field1.setAccessible(true);
        ConcurrentSkipListMap<ManagedTrashImpl.TrashKey, LedgerInfo> trashData =
                (ConcurrentSkipListMap<ManagedTrashImpl.TrashKey, LedgerInfo>) field1.get(managedTrash);

        assertEquals(trashData.size(), 30);

        Optional<Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo>> lastLedgerTrashData =
                trashData.descendingMap().entrySet().stream()
                        .filter(ele -> ele.getKey().getType() != ManagedTrash.LedgerType.OFFLOAD_LEDGER)
                        .findFirst();
        assertTrue(lastLedgerTrashData.isPresent());
        assertTrue(lastLedgerTrashData.get().getKey().getRetryCount() < maxDeleteCount);

        byte[] content = persistedData.get("/managed-trash/managed-ledger/my_test_ledger/delete");

        NavigableMap<ManagedTrashImpl.TrashKey, LedgerInfo> persistedTrashData = managedTrash.deSerialize(content);

        assertEquals(persistedTrashData.size(), 30);

        for (int i = 0; i < persistedTrashData.size(); i++) {
            Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry1 = persistedTrashData.pollFirstEntry();
            Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry2 = trashData.pollFirstEntry();
            assertEquals(entry1.getKey().compareTo(entry2.getKey()), 0);
        }
    }
}
