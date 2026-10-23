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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class PulsarLedgerCreateRecoveryTest {
    private static final long LEDGER_ID = 123L;
    private static final LedgerMetadataSerDe SERDE = new LedgerMetadataSerDe();

    @DataProvider
    public Object[][] conflicts() {
        List<Object[]> cases = new ArrayList<>();
        for (Throwable cause : List.of(new MetadataStoreException.BadVersionException("exists"),
                new MetadataStoreException.AlreadyExistsException("exists"))) {
            cases.add(new Object[]{cause, cause});
            cases.add(new Object[]{new CompletionException(cause), cause});
            cases.add(new Object[]{new ExecutionException(cause), cause});
            cases.add(new Object[]{new CompletionException(new ExecutionException(cause)), cause});
        }
        return cases.toArray(Object[][]::new);
    }

    @Test
    public void normalCreatePersistsFreshTokenAndLedgerId() throws Exception {
        try (var fixture = new Fixture()) {
            LedgerMetadata input = LedgerMetadataBuilder.from(metadata(3)).withCToken(42L).build();
            var created = fixture.manager.createLedgerMetadata(LEDGER_ID, input).get(10, TimeUnit.SECONDS);
            LedgerMetadata stored = fixture.readPersisted();
            assertThat(created.getValue().getLedgerId()).isEqualTo(LEDGER_ID);
            assertThat(created.getValue().getCToken()).isNotZero().isNotEqualTo(input.getCToken());
            assertThat(stored.getCToken()).isEqualTo(created.getValue().getCToken());
            assertThat(created.getVersion()).isEqualTo(new LongVersion(0));
            assertThat(input.getCToken()).isEqualTo(42L);
            verify(fixture.store, never()).sync(anyString());
        }
    }

    @Test(dataProvider = "conflicts")
    public void committedCreateRecoversPersistedMetadataAndVersion(Throwable failure, Throwable cause)
            throws Exception {
        try (var fixture = new Fixture()) {
            doAnswer(invocation -> {
                byte[] data = invocation.getArgument(1);
                LedgerMetadata attempted = SERDE.parseConfig(data, LEDGER_ID, Optional.empty());
                // The backend committed the write, then retried the SAME bytes after losing the response.
                fixture.persist(data, 0);
                // A concurrent owner update makes returning the input or version zero incorrect.
                fixture.persist(SERDE.serialize(LedgerMetadataBuilder.from(attempted)
                        .withCToken(attempted.getCToken()).withClosedState()
                        .withLastEntryId(8L).withLength(100L).build()), 9);
                return CompletableFuture.failedFuture(failure);
            }).when(fixture.store).put(eq(fixture.path), any(byte[].class), eq(Optional.of(-1L)));

            var recovered = fixture.manager.createLedgerMetadata(LEDGER_ID, metadata(3)).get(10, TimeUnit.SECONDS);
            assertThat(recovered.getValue().getLedgerId()).isEqualTo(LEDGER_ID);
            assertThat(recovered.getValue().getCToken()).isNotZero().isEqualTo(fixture.readPersisted().getCToken());
            assertThat(recovered.getValue().isClosed()).isTrue();
            assertThat(recovered.getValue().getLastEntryId()).isEqualTo(8L);
            assertThat(recovered.getVersion()).isEqualTo(new LongVersion(9));
            var order = inOrder(fixture.store);
            order.verify(fixture.store).put(eq(fixture.path), any(byte[].class), eq(Optional.of(-1L)));
            order.verify(fixture.store).sync(fixture.path);
            order.verify(fixture.store).get(fixture.path);
            fixture.verifyNoExtraWrites();
        }
    }

    @Test(dataProvider = "conflicts")
    public void realCollisionPreservesMetadataAndCause(Throwable failure, Throwable cause) throws Exception {
        try (var fixture = new Fixture()) {
            fixture.persist(SERDE.serialize(metadata(3)), 5);
            GetResult original = fixture.persisted.get();
            doReturn(CompletableFuture.failedFuture(failure)).when(fixture.store)
                    .put(eq(fixture.path), any(byte[].class), eq(Optional.of(-1L)));
            assertThatThrownBy(() -> fixture.manager.createLedgerMetadata(LEDGER_ID, metadata(3))
                    .get(10, TimeUnit.SECONDS))
                    .cause().isInstanceOf(BKException.BKLedgerExistException.class)
                    .satisfies(error -> assertThat(error.getCause()).isSameAs(cause));
            assertThat(fixture.persisted.get()).isSameAs(original);
            fixture.verifyNoExtraWrites();
        }
    }

    @DataProvider
    public Object[][] cacheStates() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "cacheStates")
    public void recoveryBypassesAndInvalidatesStaleCache(boolean positive) throws Exception {
        try (var fixture = new Fixture()) {
            if (positive) {
                fixture.persist(SERDE.serialize(metadata(3)), 2);
                fixture.manager.readLedgerMetadata(LEDGER_ID).get(10, TimeUnit.SECONDS);
            } else {
                assertThatThrownBy(() -> fixture.manager.readLedgerMetadata(LEDGER_ID).get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(BKException.BKNoSuchLedgerExistsOnMetadataServerException.class);
            }
            // The fake backend intentionally sends no notifications. The real cache remains stale.
            doAnswer(invocation -> {
                fixture.persist(invocation.getArgument(1), 7);
                return CompletableFuture.failedFuture(new MetadataStoreException.BadVersionException("exists"));
            }).when(fixture.store).put(eq(fixture.path), any(byte[].class), eq(Optional.of(-1L)));
            var recovered = fixture.manager.createLedgerMetadata(LEDGER_ID, metadata(3)).get(10, TimeUnit.SECONDS);
            var read = fixture.manager.readLedgerMetadata(LEDGER_ID).get(10, TimeUnit.SECONDS);
            assertThat(read.getVersion()).isEqualTo(recovered.getVersion()).isEqualTo(new LongVersion(7));
            assertThat(read.getValue().getCToken()).isNotZero().isEqualTo(recovered.getValue().getCToken());
        }
    }

    @DataProvider
    public Object[][] readFailures() {
        return new Object[][]{{"missing"}, {"read"}, {"sync"}, {"malformed"}, {"syncThrow"}, {"readThrow"},
                {"readExists"}, {"syncExists"}};
    }

    @Test(dataProvider = "readFailures")
    public void uncertainReadBackFailsWithoutModifyingData(String mode) throws Exception {
        try (var fixture = new Fixture()) {
            doReturn(CompletableFuture.failedFuture(new MetadataStoreException.BadVersionException("exists")))
                    .when(fixture.store).put(eq(fixture.path), any(byte[].class), eq(Optional.of(-1L)));
            Throwable readFailure = mode.endsWith("Exists")
                    ? new MetadataStoreException.AlreadyExistsException("read-back unavailable")
                    : new MetadataStoreException("read-back unavailable");
            Class<? extends Throwable> expected = mode.endsWith("Exists")
                    ? BKException.ZKException.class : MetadataStoreException.class;
            switch (mode) {
                case "missing":
                    expected = BKException.BKNoSuchLedgerExistsOnMetadataServerException.class;
                    break;
                case "read":
                case "readExists":
                    doReturn(CompletableFuture.failedFuture(
                            new CompletionException(new ExecutionException(readFailure))))
                            .when(fixture.store).get(fixture.path);
                    break;
                case "sync":
                case "syncExists":
                    doReturn(CompletableFuture.failedFuture(readFailure)).when(fixture.store).sync(fixture.path);
                    break;
                case "syncThrow":
                    doAnswer(invocation -> {
                        throw readFailure;
                    }).when(fixture.store).sync(fixture.path);
                    break;
                case "readThrow":
                    doAnswer(invocation -> {
                        throw readFailure;
                    }).when(fixture.store).get(fixture.path);
                    break;
                case "malformed":
                    fixture.persist(new byte[]{1, 2, 3}, 3);
                    expected = BKException.BKMetadataSerializationException.class;
                    break;
                default:
                    throw new AssertionError(mode);
            }
            GetResult original = fixture.persisted.get();
            assertThatThrownBy(() -> fixture.manager.createLedgerMetadata(LEDGER_ID, metadata(3))
                    .get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(expected)
                    .satisfies(error -> {
                        if (!mode.equals("missing") && !mode.equals("malformed")) {
                            assertThat(error).rootCause().isSameAs(readFailure);
                            assertThat(BKException.getExceptionCode(error.getCause()))
                                    .isNotEqualTo(BKException.Code.LedgerExistException);
                        }
                    });
            assertThat(fixture.persisted.get()).isSameAs(original);
            if (mode.startsWith("sync")) {
                verify(fixture.store, never()).get(fixture.path);
            }
            fixture.verifyNoExtraWrites();
        }
    }

    @DataProvider
    public Object[][] legacyFormats() {
        return new Object[][]{{1}, {2}};
    }

    @Test(dataProvider = "legacyFormats")
    public void legacySerializationAndConflictMappingRemainUnchanged(int format) throws Exception {
        try (var fixture = new Fixture()) {
            LedgerMetadata input = metadata(format);
            var created = fixture.manager.createLedgerMetadata(LEDGER_ID, input).get(10, TimeUnit.SECONDS);
            assertThat(created.getValue()).isSameAs(input);
            assertThat(fixture.persisted.get().getValue()).isEqualTo(SERDE.serialize(input));
            assertThat(fixture.readPersisted().getCToken()).isZero();
            assertThatThrownBy(() -> fixture.manager.createLedgerMetadata(LEDGER_ID, input).get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKLedgerExistException.class);
            doReturn(CompletableFuture.failedFuture(new MetadataStoreException.AlreadyExistsException("exists")))
                    .when(fixture.store).put(eq(fixture.path), any(byte[].class), eq(Optional.of(-1L)));
            assertThatThrownBy(() -> fixture.manager.createLedgerMetadata(LEDGER_ID, input).get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKLedgerExistException.class);
            verify(fixture.store, never()).sync(anyString());
            verify(fixture.store, never()).get(anyString());
        }
    }

    @Test
    public void independentInvocationsUseDifferentTokens() throws Exception {
        try (var fixture = new Fixture()) {
            LedgerMetadata input = metadata(3);
            long first = fixture.manager.createLedgerMetadata(LEDGER_ID, input).get(10, TimeUnit.SECONDS)
                    .getValue().getCToken();
            fixture.persisted.set(null);
            long second = fixture.manager.createLedgerMetadata(LEDGER_ID, input).get(10, TimeUnit.SECONDS)
                    .getValue().getCToken();
            assertThat(first).isNotZero().isNotEqualTo(second);
            assertThat(second).isNotZero();
            assertThat(input.getCToken()).isZero();
        }
    }

    @DataProvider
    public Object[][] nonConflictFailures() {
        List<Object[]> cases = new ArrayList<>();
        for (Throwable cause : List.of(new TimeoutException("response lost without a conflict"),
                new MetadataStoreException("store unavailable"))) {
            cases.add(new Object[]{cause, cause});
            cases.add(new Object[]{new CompletionException(new ExecutionException(cause)), cause});
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "nonConflictFailures")
    public void nonConflictFailureDoesNotTriggerRecovery(Throwable failure, Throwable cause) throws Exception {
        try (var fixture = new Fixture()) {
            doReturn(CompletableFuture.failedFuture(failure)).when(fixture.store)
                    .put(eq(fixture.path), any(byte[].class), eq(Optional.of(-1L)));
            assertThatThrownBy(() -> fixture.manager.createLedgerMetadata(LEDGER_ID, metadata(3))
                    .get(10, TimeUnit.SECONDS))
                    .satisfies(error -> assertThat(BKException.getExceptionCode(error.getCause()))
                            .isNotEqualTo(BKException.Code.LedgerExistException))
                    .rootCause().isSameAs(cause);
            verify(fixture.store, never()).sync(anyString());
            verify(fixture.store, never()).get(anyString());
        }
    }

    static LedgerMetadata metadata(int format) {
        return LedgerMetadataBuilder.create().withId(0L).withMetadataFormatVersion(format)
                .withEnsembleSize(1).withWriteQuorumSize(1).withAckQuorumSize(1)
                .withPassword(new byte[0]).withDigestType(DigestType.CRC32C)
                .newEnsembleEntry(0L, List.of(BookieId.parse("bookie:3181"))).build();
    }

    private static class Fixture implements AutoCloseable {
        final MetadataStore store = mock(MetadataStore.class);
        final AtomicReference<GetResult> persisted = new AtomicReference<>();
        final OrderedExecutor executor = OrderedExecutor.newBuilder().numThreads(1).name("create-recovery").build();
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        final PulsarLedgerManager manager;
        final String path;

        Fixture() {
            doAnswer(invocation -> {
                MetadataSerde<LedgerMetadata> serde = invocation.getArgument(0);
                return new MetadataCacheImpl<>("create-recovery-" + UUID.randomUUID(), store, serde,
                        MetadataCacheConfig.<LedgerMetadata>builder().refreshAfterWriteMillis(0)
                                .expireAfterWriteMillis(0).build(), executor, scheduler);
            }).when(store).getMetadataCache(any(MetadataSerde.class));
            // The manager cancels its store listener on close.
            doReturn((Runnable) () -> { }).when(store).registerCancellableListener(any());
            manager = new PulsarLedgerManager(store, "/ledgers");
            path = manager.getLedgerPath(LEDGER_ID);
            doReturn(CompletableFuture.completedFuture(null)).when(store).sync(path);
            doAnswer(invocation -> CompletableFuture.completedFuture(Optional.ofNullable(persisted.get())))
                    .when(store).get(path);
            doAnswer(invocation -> {
                if (persisted.get() != null) {
                    return CompletableFuture.failedFuture(new MetadataStoreException.BadVersionException("exists"));
                }
                persist(invocation.getArgument(1), 0);
                return CompletableFuture.completedFuture(persisted.get().getStat());
            }).when(store).put(eq(path), any(byte[].class), eq(Optional.of(-1L)));
        }

        void persist(byte[] data, long version) {
            persisted.set(new GetResult(data.clone(), new Stat(path, version, 1000L, 2000L, false, true)));
        }

        LedgerMetadata readPersisted() throws IOException {
            return SERDE.parseConfig(persisted.get().getValue(), LEDGER_ID, Optional.of(1000L));
        }

        void verifyNoExtraWrites() {
            verify(store, times(1)).put(eq(path), any(byte[].class), eq(Optional.of(-1L)));
            verify(store, never()).delete(anyString(), any());
        }

        @Override
        public void close() throws Exception {
            manager.close();
            executor.shutdownNow();
            scheduler.shutdownNow();
        }
    }
}
