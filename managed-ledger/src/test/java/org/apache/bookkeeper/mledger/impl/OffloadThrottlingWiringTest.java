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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.OffloadReadHandle;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.testng.annotations.Test;

/**
 * Pins the wiring between the factory offload token bucket and the read handles the offloader
 * receives: the handle is wrapped only when offload read throttling is enabled.
 */
public class OffloadThrottlingWiringTest extends MockedBookKeeperTestCase {

    // a rate high enough that a read only waits for the first token refill
    private static final long OFFLOAD_FLOW_PERMITS_PER_SECOND = 1024L * 1024 * 1024;

    @Override
    protected void initManagedLedgerFactoryConfig(ManagedLedgerFactoryConfig config) {
        super.initManagedLedgerFactoryConfig(config);
        config.setManagedLedgerOffloadFlowPermitsPerSecond(OFFLOAD_FLOW_PERMITS_PER_SECOND);
    }

    @Test
    public void offloaderReceivesThrottledReadHandleWhenThrottlingIsEnabled() throws Exception {
        HandleReadingOffloader offloader = new HandleReadingOffloader();
        ManagedLedgerConfig config = ledgerConfig(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("throttled_offload_ledger", config);

        for (int i = 0; i < 25; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }
        Position p = ledger.getLastConfirmedEntry();

        ledger.offloadPrefix(p);

        ReadHandle readHandle = offloader.readHandle.get();
        assertNotNull(readHandle);
        assertTrue(readHandle instanceof OffloadReadHandle,
                "offloader should receive an OffloadReadHandle when throttling is enabled");
    }

    @Test
    public void offloaderReceivesRawReadHandleWhenThrottlingIsDisabled() throws Exception {
        ManagedLedgerFactoryImpl unthrottledFactory = null;
        try {
            unthrottledFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, new ManagedLedgerFactoryConfig());
            HandleReadingOffloader offloader = new HandleReadingOffloader();
            ManagedLedgerConfig config = ledgerConfig(offloader);
            ManagedLedgerImpl ledger =
                    (ManagedLedgerImpl) unthrottledFactory.open("unthrottled_offload_ledger", config);

            for (int i = 0; i < 25; i++) {
                ledger.addEntry(("entry-" + i).getBytes());
            }
            Position p = ledger.getLastConfirmedEntry();

            ledger.offloadPrefix(p);

            ReadHandle readHandle = offloader.readHandle.get();
            assertNotNull(readHandle);
            assertFalse(readHandle instanceof OffloadReadHandle,
                    "offloader should receive the raw read handle when throttling is disabled");
        } finally {
            if (unthrottledFactory != null) {
                unthrottledFactory.shutdownAsync().get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static ManagedLedgerConfig ledgerConfig(LedgerOffloader offloader) {
        ManagedLedgerConfig config = defaultConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setLedgerOffloader(offloader);
        return config;
    }

    /**
     * An offloader that records the read handle it receives and reads the first entry through it,
     * so that the test also covers a read issued via the wrapper against a real handle.
     */
    private static final class HandleReadingOffloader implements LedgerOffloader {
        final AtomicReference<ReadHandle> readHandle = new AtomicReference<>();

        @Override
        public String getOffloadDriverName() {
            return "mock";
        }

        @Override
        public OffloadPolicies getOffloadPolicies() {
            return null;
        }

        @Override
        public CompletableFuture<Void> offload(ReadHandle ledger, UUID uid, Map<String, String> extraMetadata) {
            readHandle.set(ledger);
            return ledger.readAsync(0, 0).thenApply(entries -> {
                entries.close();
                return null;
            });
        }

        @Override
        public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                Map<String, String> offloadDriverMetadata) {
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException("HandleReadingOffloader does not support read"));
        }

        @Override
        public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid,
                Map<String, String> offloadDriverMetadata) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }
    }
}
