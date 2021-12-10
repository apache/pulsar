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
package org.apache.pulsar.metadata.bookkeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.Test;

@Slf4j
public class PulsarLedgerIdGeneratorTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void testGenerateLedgerId(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        PulsarLedgerIdGenerator ledgerIdGenerator = new PulsarLedgerIdGenerator(store, "/ledgers");
        // Create *nThread* threads each generate *nLedgers* ledger id,
        // and then check there is no identical ledger id.
        final int nThread = 2;
        final int nLedgers = 2000;
        // Multiply by two. We're going to do half in the old legacy space and half in the new.
        CountDownLatch countDownLatch1 = new CountDownLatch(nThread * nLedgers);

        final AtomicInteger errCount = new AtomicInteger(0);
        final ConcurrentLinkedQueue<Long> ledgerIds = new ConcurrentLinkedQueue<Long>();

        long start = System.currentTimeMillis();

        @Cleanup(value = "shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        for (int i = 0; i < nThread; i++) {
            executor.submit(() -> {
                for (int j = 0; j < nLedgers; j++) {
                    ledgerIdGenerator.generateLedgerId((rc, result) -> {
                        if (KeeperException.Code.OK.intValue() == rc) {
                            ledgerIds.add(result);
                        } else {
                            errCount.incrementAndGet();
                        }
                        countDownLatch1.countDown();
                    });
                }
            });
        }

        countDownLatch1.await();
        CountDownLatch countDownLatch2 = new CountDownLatch(nThread * nLedgers);

        // Go and create the long-id directory in zookeeper. This should cause the id generator to generate ids with the
        // new algo once we clear it's stored status.
        store.put("/ledgers/idgen-long", new byte[0], Optional.empty()).join();

        for (int i = 0; i < nThread; i++) {
            executor.submit(() -> {
                for (int j = 0; j < nLedgers; j++) {
                    ledgerIdGenerator.generateLedgerId((rc, result) -> {
                        if (KeeperException.Code.OK.intValue() == rc) {
                            ledgerIds.add(result);
                        } else {
                            errCount.incrementAndGet();
                        }
                        countDownLatch2.countDown();
                    });
                }
            });
        }

        assertTrue(countDownLatch2.await(120, TimeUnit.SECONDS),
                "Wait ledger id generation threads to stop timeout : ");
        log.info("Number of generated ledger id: {}, time used: {}", ledgerIds.size(),
                System.currentTimeMillis() - start);
        assertEquals(errCount.get(), 0, "Error occur during ledger id generation : ");

        Set<Long> ledgers = new HashSet<>();
        while (!ledgerIds.isEmpty()) {
            Long ledger = ledgerIds.poll();
            assertNotNull(ledger, "Generated ledger id is null");
            assertFalse(ledgers.contains(ledger), "Ledger id [" + ledger + "] conflict : ");
            ledgers.add(ledger);
        }
    }

    @Test(dataProvider = "impl")
    public void testEnsureCounterIsNotResetWithContainerNodes(String provider, Supplier<String> urlSupplier)
            throws Exception {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        PulsarLedgerIdGenerator ledgerIdGenerator = new PulsarLedgerIdGenerator(store, "/ledgers");

        CountDownLatch l1 = new CountDownLatch(1);
        AtomicLong res1 = new AtomicLong();
        ledgerIdGenerator.generateLedgerId((rc, result) -> {
            assertEquals(rc, BKException.Code.OK);
            res1.set(result);
            l1.countDown();
        });

        l1.await();
        log.info("res1 : {}", res1);

        zks.checkContainers();

        CountDownLatch l2 = new CountDownLatch(1);
        AtomicLong res2 = new AtomicLong();
        ledgerIdGenerator.generateLedgerId((rc, result) -> {
            assertEquals(rc, BKException.Code.OK);
            res2.set(result);
            l2.countDown();
        });
        l2.await();

        log.info("res2 : {}", res2);
        assertNotEquals(res1, res2);
        assertTrue(res1.get() < res2.get());
    }
}
