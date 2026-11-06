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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Stat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarLedgerManagerCacheTest {
    private static final long LEDGER_ID = 1L;
    private static final List<BookieId> UPDATED_ENSEMBLE = List.of(BookieId.parse("bookie-2:3181"));

    private MetadataStore store;
    private PulsarLedgerManager manager;
    private LedgerMetadata metadata;
    private String ledgerPath;
    private CompletableFuture<Stat> pendingPut;

    @BeforeMethod
    public void setup() throws Exception {
        // Use the real metadata cache, but control backend reads and writes without delivering notifications.
        store = spy(MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build()));
        manager = new PulsarLedgerManager(store, "/ledgers");
        ledgerPath = manager.getLedgerPath(LEDGER_ID);
        metadata = LedgerMetadataBuilder.create()
                .withId(LEDGER_ID)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withPassword(new byte[0])
                .withDigestType(DigestType.CRC32C)
                .newEnsembleEntry(0L, List.of(BookieId.parse("bookie-1:3181")))
                .build();
        setBackendMetadata(metadata, 7L);
        assertThat(manager.readLedgerMetadata(LEDGER_ID).get(10, TimeUnit.SECONDS).getVersion())
                .isEqualTo(new LongVersion(7L));
        pendingPut = new CompletableFuture<>();
        doReturn(pendingPut).when(store).put(eq(ledgerPath), any(byte[].class), eq(Optional.of(7L)));
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        try {
            if (manager != null) {
                manager.close();
            }
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }

    @DataProvider
    public Object[][] badVersionFailures() {
        Throwable conflict = new MetadataStoreException.BadVersionException("stale version");
        return new Object[][] {
                {conflict},
                {new CompletionException(conflict)},
                {new CompletionException(new ExecutionException(conflict))}
        };
    }

    @Test(dataProvider = "badVersionFailures", timeOut = 30000)
    public void testRetryReadsFreshMetadataOnBadVersion(Throwable failure) throws Exception {
        LedgerMetadata updatedMetadata = LedgerMetadataBuilder.from(metadata)
                .replaceEnsembleEntry(0L, UPDATED_ENSEMBLE).build();
        setBackendMetadata(updatedMetadata, 8L);
        // Confirm the read is still cached until the conflicting write fails.
        assertThat(manager.readLedgerMetadata(LEDGER_ID).get(10, TimeUnit.SECONDS).getVersion())
                .isEqualTo(new LongVersion(7L));

        CompletableFuture<Versioned<LedgerMetadata>> retryRead = manager
                .writeLedgerMetadata(LEDGER_ID, metadata, new LongVersion(7L))
                .handle((result, error) -> {
                    assertThat(error).isInstanceOf(BKException.BKMetadataVersionException.class);
                    // Register before completing the backend write: this callback runs inline on failure.
                    return manager.readLedgerMetadata(LEDGER_ID);
                }).thenCompose(read -> read);
        pendingPut.completeExceptionally(failure);

        Versioned<LedgerMetadata> fresh = retryRead.get(10, TimeUnit.SECONDS);
        assertThat(fresh.getVersion()).isEqualTo(new LongVersion(8L));
        assertThat(fresh.getValue().getEnsembleAt(0L)).isEqualTo(UPDATED_ENSEMBLE);
    }

    @Test(timeOut = 30000)
    public void testOtherWriteFailurePreservesCacheAndCause() throws Exception {
        MetadataStoreException failure = new MetadataStoreException("backend unavailable");
        CompletableFuture<Void> checked = manager.writeLedgerMetadata(LEDGER_ID, metadata, new LongVersion(7L))
                .handle((result, error) -> {
                    assertThat(error).isInstanceOf(BKException.ZKException.class).hasCause(failure);
                    return null;
                });
        pendingPut.completeExceptionally(new CompletionException(new ExecutionException(failure)));
        checked.get(10, TimeUnit.SECONDS);

        setBackendMetadata(metadata, 8L);
        assertThat(manager.readLedgerMetadata(LEDGER_ID).get(10, TimeUnit.SECONDS).getVersion())
                .isEqualTo(new LongVersion(7L));
    }

    private void setBackendMetadata(LedgerMetadata value, long version) throws Exception {
        GetResult result = new GetResult(new LedgerMetadataSerDe().serialize(value),
                new Stat(ledgerPath, version, 0L, 0L, false, false));
        doReturn(CompletableFuture.completedFuture(Optional.of(result))).when(store).get(ledgerPath);
    }
}
