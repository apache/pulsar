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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarLedgerManagerTest extends BaseMetadataStoreTest {

    @DataProvider(name = "createConflicts")
    public Object[][] createConflicts() {
        List<Object[]> cases = new ArrayList<>();
        for (int format : List.of(1, 2)) {
            for (Throwable cause : List.of(new MetadataStoreException.BadVersionException("Ledger exists"),
                    new MetadataStoreException.AlreadyExistsException("Ledger exists"))) {
                cases.add(new Object[]{format, cause, cause});
                cases.add(new Object[]{format, new CompletionException(cause), cause});
                cases.add(new Object[]{format, new ExecutionException(cause), cause});
                cases.add(new Object[]{format, new CompletionException(new ExecutionException(cause)), cause});
            }
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "createConflicts", timeOut = 30000)
    public void testCreationConflictPreservesCause(int format, Throwable failure, Throwable cause) throws Exception {
        MetadataStore store = mock(MetadataStore.class);
        doReturn((Runnable) () -> { }).when(store).registerCancellableListener(any());
        doReturn(CompletableFuture.failedFuture(failure)).when(store)
                .put(anyString(), any(byte[].class), eq(Optional.of(-1L)));
        // This exception-only mock exercises v1/v2; v3 recovery requires a persisted read-back.
        LedgerMetadata input = LedgerMetadataBuilder.from(metadata(123L, "value"))
                .withMetadataFormatVersion(format).build();
        try (var manager = new PulsarLedgerManager(store, "/ledgers")) {
            assertThatThrownBy(() -> manager.createLedgerMetadata(123L, input)
                    .get(10, TimeUnit.SECONDS))
                    .cause().isInstanceOf(BKException.BKLedgerExistException.class)
                    .satisfies(error -> assertThat(error.getCause()).isSameAs(cause));
            verify(store, never()).sync(anyString());
            verify(store, never()).get(anyString());
        }
    }

    @DataProvider(name = "nonConflictFailures")
    public Object[][] nonConflictFailures() {
        List<Object[]> cases = new ArrayList<>();
        for (Throwable cause : List.of(new MetadataStoreException("Store unavailable"),
                new TimeoutException("Store request timed out"))) {
            cases.add(new Object[]{cause, cause});
            cases.add(new Object[]{new CompletionException(new ExecutionException(cause)), cause});
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "nonConflictFailures", timeOut = 30000)
    public void testOtherCreationFailuresAreNotLedgerConflicts(Throwable failure, Throwable cause) throws Exception {
        MetadataStore store = mock(MetadataStore.class);
        doReturn((Runnable) () -> { }).when(store).registerCancellableListener(any());
        doReturn(CompletableFuture.failedFuture(failure)).when(store)
                .put(anyString(), any(byte[].class), eq(Optional.of(-1L)));
        try (var manager = new PulsarLedgerManager(store, "/ledgers")) {
            assertThatThrownBy(() -> manager.createLedgerMetadata(123L, metadata(123L, "value"))
                    .get(10, TimeUnit.SECONDS)).cause().satisfies(error -> {
                        assertThat(BKException.getExceptionCode(error))
                                .as("Non-conflict failures must not trigger ledger ID regeneration")
                                .isNotEqualTo(BKException.Code.LedgerExistException);
                        assertThat(ExceptionUtils.getThrowableList(error))
                                .as("The metadata store failure remains in the cause chain")
                                .contains(cause);
                    });
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testDuplicateCreationPreservesExistingMetadata(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (var store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
             var manager = new PulsarLedgerManager(store, "/ledgers-" + UUID.randomUUID())) {
            long ledgerId = 123L;
            var original = manager.createLedgerMetadata(ledgerId, metadata(ledgerId, "original"))
                    .get(10, TimeUnit.SECONDS);

            assertThatThrownBy(() -> manager.createLedgerMetadata(ledgerId, metadata(ledgerId, "replacement"))
                    .get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKLedgerExistException.class);

            var persisted = manager.readLedgerMetadata(ledgerId).get(10, TimeUnit.SECONDS);
            assertThat(persisted.getVersion()).isEqualTo(original.getVersion());
            assertThat(persisted.getValue().getCustomMetadata().get("value"))
                    .isEqualTo("original".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testStaleVersionsRemainMetadataVersionErrors(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (var store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
             var manager = new PulsarLedgerManager(store, "/ledgers-" + UUID.randomUUID())) {
            long ledgerId = 123L;
            var original = manager.createLedgerMetadata(ledgerId, metadata(ledgerId, "original"))
                    .get(10, TimeUnit.SECONDS);
            manager.writeLedgerMetadata(ledgerId, metadata(ledgerId, "updated"), original.getVersion())
                    .get(10, TimeUnit.SECONDS);

            assertThatThrownBy(() -> manager.writeLedgerMetadata(ledgerId, original.getValue(), original.getVersion())
                    .get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKMetadataVersionException.class);
            assertThatThrownBy(() -> manager.removeLedgerMetadata(ledgerId, original.getVersion())
                    .get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKMetadataVersionException.class);

            assertThat(manager.readLedgerMetadata(ledgerId).get(10, TimeUnit.SECONDS)
                    .getValue().getCustomMetadata().get("value"))
                    .isEqualTo("updated".getBytes(StandardCharsets.UTF_8));
        }
    }

    private static LedgerMetadata metadata(long ledgerId, String value) {
        return LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .withCustomMetadata(Map.of("value", value.getBytes(StandardCharsets.UTF_8)))
                .newEnsembleEntry(0L, List.of(BookieId.parse("bookie:3181")))
                .build();
    }
}
