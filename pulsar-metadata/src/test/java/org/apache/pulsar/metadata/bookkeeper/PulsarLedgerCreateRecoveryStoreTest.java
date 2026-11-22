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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class PulsarLedgerCreateRecoveryStoreTest extends BaseMetadataStoreTest {
    @Test(dataProvider = "impl", timeOut = 60000)
    public void committedCreateThenBackendRetryRecoversSameId(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (var store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build())) {
            MetadataStore faultingStore = committedThenRetried(store);
            try (var manager = new PulsarLedgerManager(faultingStore, "/ledgers-" + UUID.randomUUID())) {
                long id = 123L;
                var recovered = manager.createLedgerMetadata(id, PulsarLedgerCreateRecoveryTest.metadata(3))
                        .get(10, TimeUnit.SECONDS);
                var persisted = store.get(manager.getLedgerPath(id)).get(10, TimeUnit.SECONDS).orElseThrow();
                LedgerMetadata metadata = new LedgerMetadataSerDe().parseConfig(persisted.getValue(), id,
                        Optional.of(persisted.getStat().getCreationTimestamp()));
                assertThat(recovered.getValue().getLedgerId()).isEqualTo(id);
                assertThat(recovered.getValue().getCToken()).isNotZero().isEqualTo(metadata.getCToken());
                assertThat(manager.readLedgerMetadata(id).get(10, TimeUnit.SECONDS).getVersion())
                        .isEqualTo(recovered.getVersion());
                verify(faultingStore, times(1)).put(anyString(), any(byte[].class), eq(Optional.of(-1L)));
            }
        }
    }

    @Test(dataProvider = "impl", timeOut = 60000)
    public void genuineCollisionAndStaleUpdateDeletePreserveData(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (var store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
             var manager = new PulsarLedgerManager(store, "/ledgers-" + UUID.randomUUID())) {
            long id = 123L;
            var original = manager.createLedgerMetadata(id, PulsarLedgerCreateRecoveryTest.metadata(3))
                    .get(10, TimeUnit.SECONDS);
            var bytes = store.get(manager.getLedgerPath(id)).get(10, TimeUnit.SECONDS).orElseThrow().getValue();
            assertThatThrownBy(() -> manager.createLedgerMetadata(id, original.getValue()).get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKLedgerExistException.class);
            assertThat(store.get(manager.getLedgerPath(id)).get(10, TimeUnit.SECONDS).orElseThrow().getValue())
                    .isEqualTo(bytes);
            LedgerMetadata updated = LedgerMetadataBuilder.from(original.getValue())
                    .withCToken(original.getValue().getCToken())
                    .withClosedState().withLastEntryId(0L).withLength(10L).build();
            var written = manager.writeLedgerMetadata(id, updated, original.getVersion()).get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> manager.writeLedgerMetadata(id, original.getValue(), original.getVersion())
                    .get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(BKException.BKMetadataVersionException.class);
            assertThatThrownBy(() -> manager.removeLedgerMetadata(id, original.getVersion()).get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKMetadataVersionException.class);
            manager.readLedgerMetadata(id).get(10, TimeUnit.SECONDS);
            var persisted = store.get(manager.getLedgerPath(id)).get(10, TimeUnit.SECONDS).orElseThrow();
            assertThat(persisted.getValue()).isEqualTo(new LedgerMetadataSerDe().serialize(updated));
            manager.removeLedgerMetadata(id, written.getVersion()).get(10, TimeUnit.SECONDS);
            assertThat(store.get(manager.getLedgerPath(id)).get(10, TimeUnit.SECONDS)).isEmpty();
        }
    }

    @Test(dataProvider = "impl", timeOut = 60000)
    public void ledgerCreateOpDoesNotAllocateReplacementId(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (var cluster = BKCluster.builder().metadataServiceUri(urlSupplier.get()).build();
             var client = spy(cluster.newClient());
             var manager = new PulsarLedgerManager(committedThenRetried(cluster.getStore()), "/ledgers")) {
            LedgerManager trackingManager = spy(manager);
            AtomicLong attemptedId = new AtomicLong(-1L);
            doAnswer(invocation -> {
                long id = invocation.getArgument(0);
                if (!attemptedId.compareAndSet(-1L, id)) {
                    return CompletableFuture.failedFuture(new AssertionError("Unexpected replacement ledger ID"));
                }
                return manager.createLedgerMetadata(id, invocation.getArgument(1));
            }).when(trackingManager).createLedgerMetadata(anyLong(), any(LedgerMetadata.class));
            doReturn(trackingManager).when(client).getLedgerManager();
            byte[] entry = "recovered-ledger".getBytes(StandardCharsets.UTF_8);
            long id;
            try (var handle = client.newCreateLedgerOp().withEnsembleSize(1).withWriteQuorumSize(1)
                    .withAckQuorumSize(1).withDigestType(DigestType.CRC32C).withPassword(new byte[0])
                    .execute().get(10, TimeUnit.SECONDS)) {
                id = handle.getId();
                assertThat(id).isEqualTo(attemptedId.get());
                assertThat(handle.append(entry)).isZero();
                verify(trackingManager, times(1)).createLedgerMetadata(anyLong(), any(LedgerMetadata.class));
            }
            try (var handle = client.newOpenLedgerOp().withLedgerId(id).withPassword(new byte[0])
                    .execute().get(10, TimeUnit.SECONDS);
                 var entries = handle.read(0, 0)) {
                assertThat(entries.getEntry(0).getEntryBytes()).isEqualTo(entry);
            }
        }
    }

    private static MetadataStore committedThenRetried(MetadataStore store) {
        MetadataStore faulting = mock(MetadataStore.class, delegatesTo(store));
        doAnswer(invocation -> {
            String path = invocation.getArgument(0);
            byte[] bytes = invocation.getArgument(1);
            // Simulate CONNECTIONLOSS after commit, followed by the backend's retry of the original request.
            return store.put(path, bytes, Optional.of(-1L)).thenCompose(committed ->
                    store.put(path, bytes, Optional.of(-1L)).handle((ignored, error) -> {
                        assertThat(error).isNotNull();
                        Throwable cause = FutureUtil.unwrapCompletionException(error);
                        assertThat(cause).isInstanceOfAny(MetadataStoreException.BadVersionException.class,
                                MetadataStoreException.AlreadyExistsException.class);
                        throw new CompletionException(cause);
                    }));
        }).when(faulting).put(anyString(), any(byte[].class), eq(Optional.of(-1L)));
        return faulting;
    }
}
