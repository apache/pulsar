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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Slf4j
public class PulsarLedgerAuditorManagerTest extends BaseMetadataStoreTest {

    private static final int managerVersion = 0xabcd;

    private MetadataStoreExtended store1;
    private MetadataStoreExtended store2;

    private String ledgersRootPath;


    private void methodSetup(Supplier<String> urlSupplier) throws Exception {
        this.ledgersRootPath = "/ledgers-" + UUID.randomUUID();
        this.store1 = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        this.store2 = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
    }

    @AfterMethod(alwaysRun = true)
    public final void methodCleanup() throws Exception {
        if (store1 != null) {
            store1.close();
            store1 = null;
        }

        if (store2 != null) {
            store2.close();
            store2 = null;
        }
    }

    @Test(dataProvider = "impl")
    public void testSimple(String provider, Supplier<String> urlSupplier) throws Exception {
        if (provider.equals("Memory") || provider.equals("RocksDB")) {
            // With memory provider there are not multiple sessions to test with
            return;
        }

        methodSetup(urlSupplier);

        LedgerAuditorManager lam1 = new PulsarLedgerAuditorManager(store1, ledgersRootPath);
        assertNull(lam1.getCurrentAuditor());

        lam1.tryToBecomeAuditor("bookie-1:3181", auditorEvent -> {
            log.info("---- LAM-1 - Received auditor event: {}", auditorEvent);
        });

        assertEquals(BookieId.parse("bookie-1:3181"), lam1.getCurrentAuditor());

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        LedgerAuditorManager lam2 = new PulsarLedgerAuditorManager(store2, ledgersRootPath);
        assertEquals(BookieId.parse("bookie-1:3181"), lam2.getCurrentAuditor());

        CountDownLatch latch = new CountDownLatch(1);
        executor.execute(() -> {
            try {
                lam2.tryToBecomeAuditor("bookie-2:3181", auditorEvent -> {
                    log.info("---- LAM-2 - Received auditor event: {}", auditorEvent);
                });

                // Lam2 is now auditor
                latch.countDown();
            } catch (Exception e) {
                log.error("---- Failed to become auditor", e);
            }
        });

        // LAM2 will be kept waiting until LAM1 goes away
        assertFalse(latch.await(1, TimeUnit.SECONDS));

        assertEquals(BookieId.parse("bookie-1:3181"), lam1.getCurrentAuditor());
        assertEquals(BookieId.parse("bookie-1:3181"), lam2.getCurrentAuditor());

        lam1.close();

        // Now LAM2 will take over
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(BookieId.parse("bookie-2:3181"), lam2.getCurrentAuditor());
    }

}
