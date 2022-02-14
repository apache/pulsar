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


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ManagedLedgerFactoryShutdownTest {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerFactoryShutdownTest.class);

    @Test(timeOut = 5000)
    public void openEncounteredShutdown() throws Exception {
        final String ledgerName = UUID.randomUUID().toString();
        final long version = 0;
        final long createTimeMillis = System.currentTimeMillis();

        MetadataStoreExtended metadataStore = mock(MetadataStoreExtended.class);
        CountDownLatch slowZk = new CountDownLatch(1);
        given(metadataStore.get(any())).willAnswer(inv -> {
            String path = inv.getArgument(0, String.class);
            if (path == null) {
                throw new IllegalArgumentException("Path is null.");
            }
            if (path.endsWith(ledgerName)) { // ledger
                MLDataFormats.ManagedLedgerInfo.Builder mli = MLDataFormats.ManagedLedgerInfo.newBuilder()
                        .addLedgerInfo(0, MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder()
                                .setLedgerId(0)
                                .setEntries(0)
                                .setTimestamp(System.currentTimeMillis()));
                Stat stat = new Stat(path, version, createTimeMillis, createTimeMillis, false, false);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        slowZk.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    MLDataFormats.ManagedLedgerInfo managedLedgerInfo = mli.build();
                    log.info("metadataStore.get({}) returned,managedLedgerInfo={},stat={}", path, managedLedgerInfo,
                            stat);
                    return Optional.of(new GetResult(managedLedgerInfo.toByteArray(), stat));
                });

            } else if (path.contains(ledgerName)) { // cursor
                MLDataFormats.ManagedCursorInfo.Builder mci = MLDataFormats.ManagedCursorInfo.newBuilder()
                        .setCursorsLedgerId(-1)
                        .setMarkDeleteLedgerId(0)
                        .setMarkDeleteLedgerId(-1);
                Stat stat = new Stat(path, version, createTimeMillis, createTimeMillis, false, false);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        slowZk.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    MLDataFormats.ManagedCursorInfo managedCursorInfo = mci.build();
                    log.info("metadataStore.get({}) returned:managedCursorInfo={},stat={}", path, managedCursorInfo,
                            stat);
                    return Optional.of(new GetResult(managedCursorInfo.toByteArray(), stat));
                });

            } else {
                throw new IllegalArgumentException("Invalid path: " + path);
            }
        });
        given(metadataStore.put(anyString(), any(), any())).willAnswer(inv -> {
            Optional<Long> expectedVersion = inv.getArgument(2, Optional.class);
            return CompletableFuture.supplyAsync(() -> new Stat(inv.getArgument(0, String.class),
                    expectedVersion.orElse(0L) + 1, createTimeMillis,
                    System.currentTimeMillis(), false, false));
        });
        given(metadataStore.getChildren(anyString()))
                .willAnswer(inv -> CompletableFuture.supplyAsync(() -> Collections.singletonList("cursor")));

        BookKeeper bookKeeper = mock(BookKeeper.class);
        LedgerHandle ledgerHandle = mock(LedgerHandle.class);
        LedgerHandle newLedgerHandle = mock(LedgerHandle.class);
        OrderedExecutor executor = OrderedExecutor.newBuilder().name("Test").build();
        given(bookKeeper.getMainWorkerPool()).willReturn(executor);
        doAnswer(inv -> {
            AsyncCallback.OpenCallback cb = inv.getArgument(3, AsyncCallback.OpenCallback.class);
            cb.openComplete(0, ledgerHandle, inv.getArgument(4, Object.class));
            return null;
        }).when(bookKeeper).asyncOpenLedger(anyLong(), any(), any(), any(), any());
        doAnswer(inv -> {
            AsyncCallback.CreateCallback cb = inv.getArgument(5, AsyncCallback.CreateCallback.class);
            cb.createComplete(0, newLedgerHandle, inv.getArgument(6, Object.class));
            return null;
        }).when(bookKeeper)
                .asyncCreateLedger(anyInt(), anyInt(), anyInt(), any(), any(), any()/*callback*/, any(), any());

        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bookKeeper);
        CountDownLatch callbackInvoked = new CountDownLatch(2);
        factory.asyncOpen(ledgerName, new AsyncCallbacks.OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                callbackInvoked.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                callbackInvoked.countDown();
            }
        }, null);

        factory.asyncOpenReadOnlyCursor(ledgerName, PositionImpl.EARLIEST, new ManagedLedgerConfig(),
                new AsyncCallbacks.OpenReadOnlyCursorCallback() {
                    @Override
                    public void openReadOnlyCursorComplete(ReadOnlyCursor cursor, Object ctx) {
                        callbackInvoked.countDown();
                    }

                    @Override
                    public void openReadOnlyCursorFailed(ManagedLedgerException exception, Object ctx) {
                        log.info("openReadOnlyCursorFailed");
                        callbackInvoked.countDown();
                    }
                }, null);

        log.info("Shutdown factory...");


        factory.shutdownAsync().get();
        //make zk returned after factory shutdown
        slowZk.countDown();

        //
        Assert.assertTrue(callbackInvoked.await(5, TimeUnit.SECONDS));
        //ManagedLedgerFactoryClosedException should be thrown after factory is shutdown
        Assert.assertThrows(ManagedLedgerException.ManagedLedgerFactoryClosedException.class,
                () -> factory.open(ledgerName));
        Assert.assertThrows(ManagedLedgerException.ManagedLedgerFactoryClosedException.class,
                () -> factory.openReadOnlyCursor(ledgerName, PositionImpl.EARLIEST, new ManagedLedgerConfig()));
    }
}
