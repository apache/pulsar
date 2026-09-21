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
package org.apache.pulsar.metadata.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test the Bookie and Client end-to-end with MetadataStore backend.
 */
@CustomLog
public class EndToEndTest extends BaseMetadataStoreTest {
    @Test(dataProvider = "impl", timeOut = 60000)
    public void testCreateLedgerRetriesIdCollision(String provider, Supplier<String> urlSupplier) throws Exception {
        try (var cluster = BKCluster.builder().metadataServiceUri(urlSupplier.get()).build();
             var client = spy(cluster.newClient())) {
            LedgerManager ledgerManager = client.getLedgerManager();
            LedgerManager collidingManager = spy(ledgerManager);
            AtomicLong collidingLedgerId = new AtomicLong(-1L);
            doAnswer(invocation -> {
                long ledgerId = invocation.getArgument(0);
                LedgerMetadata metadata = invocation.getArgument(1);
                if (collidingLedgerId.compareAndSet(-1L, ledgerId)) {
                    // Persist the first generated ID before the real create, producing a store-level collision.
                    return ledgerManager.createLedgerMetadata(ledgerId, metadata)
                            .thenCompose(ignored -> ledgerManager.createLedgerMetadata(ledgerId, metadata));
                }
                return ledgerManager.createLedgerMetadata(ledgerId, metadata);
            }).when(collidingManager).createLedgerMetadata(anyLong(), any(LedgerMetadata.class));
            doReturn(collidingManager).when(client).getLedgerManager();

            try (var handle = client.newCreateLedgerOp()
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withAckQuorumSize(1)
                    .withDigestType(DigestType.CRC32C)
                    .withPassword(new byte[0])
                    .execute().get(10, TimeUnit.SECONDS)) {
                assertThat(handle.getId()).isNotEqualTo(collidingLedgerId.get());
                assertThat(handle.append("entry".getBytes(StandardCharsets.UTF_8))).isZero();
                verify(collidingManager, times(2)).createLedgerMetadata(anyLong(), any(LedgerMetadata.class));
                assertThat(ledgerManager.readLedgerMetadata(collidingLedgerId.get()).get(10, TimeUnit.SECONDS)
                        .getValue().getLedgerId()).isEqualTo(collidingLedgerId.get());
            }
        }
    }

    @Test(dataProvider = "impl")
    public void testBasic(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        BKCluster bktc = BKCluster.builder()
                .metadataServiceUri(urlSupplier.get())
                .build();

        @Cleanup
        BookKeeper bkc = bktc.newClient();

        long ledgerId;
        {
            @Cleanup
            WriteHandle wh = bkc.newCreateLedgerOp()
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withAckQuorumSize(1)
                    .withDigestType(DigestType.CRC32C)
                    .withPassword(new byte[0])
                    .execute()
                    .join();

            for (int i = 0; i < 10; i++) {
                wh.append(("entry-" + i).getBytes(StandardCharsets.UTF_8));
                log.info().attr("entry", i).log("Written entry");
            }

            ledgerId = wh.getId();
        }

        @Cleanup
        ReadHandle rh = bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword(new byte[0])
                .execute()
                .join();

        @Cleanup
        LedgerEntries les = rh.read(0, 9);
        int i = 0;
        for (LedgerEntry le : les) {
            Assert.assertEquals(new String(le.getEntryBytes()), "entry-" + i++);
        }
    }


    @Test(dataProvider = "impl")
    public void testWithLedgerRecovery(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        BKCluster bktc = BKCluster.builder()
                .metadataServiceUri(urlSupplier.get())
                .numBookies(3)
                .build();

        @Cleanup
        BookKeeper bkc = bktc.newClient();

        @Cleanup
        WriteHandle wh = bkc.newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(2)
                .withAckQuorumSize(2)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute()
                .join();

        for (int i = 0; i < 10; i++) {
            wh.append(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        long ledgerId = wh.getId();

        @Cleanup
        ReadHandle rh = bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword(new byte[0])
                .withRecovery(true)
                .execute()
                .join();

        Assert.assertEquals(rh.getLastAddConfirmed(), 9L);

        @Cleanup
        LedgerEntries les = rh.read(0, 9);
        int i = 0;
        for (LedgerEntry le : les) {
            Assert.assertEquals(new String(le.getEntryBytes()), "entry-" + i++);
        }

        try {
            wh.append("test".getBytes(StandardCharsets.UTF_8));
            Assert.fail("should have failed since the ledger is fenced");
        } catch (BKException.BKLedgerFencedException fe) {
            // Expected
        }
    }
}
