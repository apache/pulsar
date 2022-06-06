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

    @Test
    public void testTrashKeyOrder() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setDeleteIntervalSeconds(1);
        managedLedgerConfig.setArchiveDataLimitSize(10);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);

        managedTrash.appendLedgerTrashData(10, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(6, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(100, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(3, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(7, null, ManagedTrash.LedgerType.LEDGER);


        LedgerInfo.Builder builder = LedgerInfo.newBuilder().setLedgerId(7);
        UUID uuid = UUID.randomUUID();
        builder.getOffloadContextBuilder()
                .setUidMsb(uuid.getMostSignificantBits())
                .setUidLsb(uuid.getLeastSignificantBits());
        LedgerInfo ledgerInfo = builder.build();
        managedTrash.appendLedgerTrashData(7, ledgerInfo, ManagedTrash.LedgerType.OFFLOAD_LEDGER);

        Field field1 = ManagedTrashImpl.class.getDeclaredField("trashData");
        field1.setAccessible(true);
        ConcurrentSkipListMap map = (ConcurrentSkipListMap) field1.get(managedTrash);

        Map.Entry<ManagedTrashImpl.TrashKey, LedgerInfo> entry = map.pollFirstEntry();
        Assert.assertEquals(entry.getKey().getLedgerId(), 3);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        entry = map.pollFirstEntry();
        Assert.assertEquals(entry.getKey().getLedgerId(), 6);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

        entry = map.pollFirstEntry();
        Assert.assertEquals(entry.getKey().getLedgerId(), 7);
        Assert.assertEquals(entry.getKey().getMsb(), uuid.getMostSignificantBits());
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.OFFLOAD_LEDGER);

        entry = map.pollFirstEntry();
        Assert.assertEquals(entry.getKey().getLedgerId(), 7);
        Assert.assertEquals(entry.getKey().getMsb(), 0);
        Assert.assertEquals(entry.getKey().getType(), ManagedTrash.LedgerType.LEDGER);

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
        int ledgerCount = 30;
        int archiveLimit = 10;

        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setSupportTwoPhaseDeletion(true);
        managedLedgerConfig.setDeleteIntervalSeconds(1);
        managedLedgerConfig.setArchiveDataLimitSize(archiveLimit);
        ManagedLedger ledger = factory.open("my_test_ledger", managedLedgerConfig);

        Field field = ManagedLedgerImpl.class.getDeclaredField("managedTrash");
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);


        //the ledgerId >= 10000, it will delete failed. see line_142.
        for (int i = 0; i < ledgerCount; i++) {
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

        assertEquals(totalArchive.size(), ledgerCount);
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
        ManagedTrashImpl managedTrash = (ManagedTrashImpl) field.get(ledger);

        managedTrash.appendLedgerTrashData(10, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(6, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(100, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(3, null, ManagedTrash.LedgerType.LEDGER);
        managedTrash.appendLedgerTrashData(7, null, ManagedTrash.LedgerType.LEDGER);
        ledger.close();
        assertEquals(persistedData.size(), 2);
        //        //managed_ledger/my_test_ledger/delete
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
}
