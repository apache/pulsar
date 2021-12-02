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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
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
        final CountDownLatch countDownLatch = new CountDownLatch(nThread * nLedgers * 2);

        final AtomicInteger errCount = new AtomicInteger(0);
        final ConcurrentLinkedQueue<Long> ledgerIds = new ConcurrentLinkedQueue<Long>();
        final BookkeeperInternalCallbacks.GenericCallback<Long>
                cb = (rc, result) -> {
            if (KeeperException.Code.OK.intValue() == rc) {
                ledgerIds.add(result);
            } else {
                errCount.incrementAndGet();
            }
            countDownLatch.countDown();
        };

        long start = System.currentTimeMillis();

        for (int i = 0; i < nThread; i++) {
            new Thread(() -> {
                for (int j = 0; j < nLedgers; j++) {
                    ledgerIdGenerator.generateLedgerId(cb);
                }
            }).start();
        }

        // Go and create the long-id directory in zookeeper. This should cause the id generator to generate ids with the
        // new algo once we clear it's stored status.
        store.put("/ledgers/idgen-long", new byte[0], Optional.empty());

        for (int i = 0; i < nThread; i++) {
            new Thread(() -> {
                for (int j = 0; j < nLedgers; j++) {
                    ledgerIdGenerator.generateLedgerId(cb);
                }
            }).start();
        }

        assertTrue(countDownLatch.await(120, TimeUnit.SECONDS),
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
}
