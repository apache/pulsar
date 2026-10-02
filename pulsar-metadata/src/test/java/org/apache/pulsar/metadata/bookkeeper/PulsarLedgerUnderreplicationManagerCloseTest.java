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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarLedgerUnderreplicationManagerCloseTest extends BaseMetadataStoreTest {
    private static final List<Long> LEDGERS = List.of(1L, 2L, 3L);

    @DataProvider(name = "implWithMissingPosition")
    public Object[][] implementationsWithMissingPosition() {
        List<Object[]> cases = new ArrayList<>();
        for (Object[] implementation : implementations()) {
            cases.add(new Object[]{implementation[0], implementation[1], 0});
            cases.add(new Object[]{implementation[0], implementation[1], 1});
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "implWithMissingPosition", timeOut = 30000)
    public void testCloseContinuesAfterMissingLock(String provider, Supplier<String> urlSupplier, int missingPosition)
            throws Exception {
        String root = "/ledgers-" + UUID.randomUUID();
        try (var store = newStore(urlSupplier)) {
            var observed = spy(store);
            try (var owner = newManager(observed, root);
                 var other = newManager(store, root)) {
                acquireLedgers(owner);
                AtomicInteger attempts = new AtomicInteger();
                doAnswer(invocation -> {
                    String path = invocation.getArgument(0);
                    Optional<Long> version = invocation.getArgument(1);
                    if (attempts.getAndIncrement() == missingPosition) {
                        // Remove the actual node before the manager's delete, without relying on map iteration order.
                        return store.delete(path, version).thenCompose(__ -> store.delete(path, version));
                    }
                    return store.delete(path, version);
                }).when(observed).delete(anyString(), any());

                owner.close();

                assertThat(attempts.get()).isEqualTo(LEDGERS.size());
                assertAllUnlocked(other);
                acquireExistingLedgers(other);
                owner.close();
                assertThat(attempts.get()).isEqualTo(LEDGERS.size());
                assertAllLocked(other);
            }
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testRepeatedCloseDoesNotDeleteReacquiredLocks(String provider, Supplier<String> urlSupplier)
            throws Exception {
        String root = "/ledgers-" + UUID.randomUUID();
        try (var store = newStore(urlSupplier);
             var owner = newManager(store, root);
             var other = newManager(store, root)) {
            acquireLedgers(owner);
            owner.close();
            assertAllUnlocked(other);
            acquireExistingLedgers(other);

            owner.close();

            assertAllLocked(other);
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testRepeatedCloseDoesNotDeleteExplicitlyReacquiredLock(
            String provider, Supplier<String> urlSupplier) throws Exception {
        String root = "/ledgers-" + UUID.randomUUID();
        long ledgerId = 1L;
        try (var store = newStore(urlSupplier);
             var owner = newManager(store, root);
             var other = newManager(store, root)) {
            owner.acquireUnderreplicatedLedger(ledgerId);
            owner.close();
            assertThat(other.isLedgerBeingReplicated(ledgerId)).isFalse();
            other.acquireUnderreplicatedLedger(ledgerId);

            owner.close();

            assertThat(other.isLedgerBeingReplicated(ledgerId)).isTrue();
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testCloseAggregatesFailuresAndRetriesOnlyUnreleasedLocks(String provider, Supplier<String> urlSupplier)
            throws Exception {
        String root = "/ledgers-" + UUID.randomUUID();
        try (var store = newStore(urlSupplier)) {
            var observed = spy(store);
            try (var owner = newManager(observed, root);
                 var other = newManager(store, root)) {
                acquireLedgers(owner);
                List<String> attemptedPaths = new ArrayList<>();
                List<MetadataStoreException> failures = List.of(new MetadataStoreException("first failure"),
                        new MetadataStoreException("second failure"));
                doAnswer(invocation -> {
                    String path = invocation.getArgument(0);
                    attemptedPaths.add(path);
                    if (attemptedPaths.size() <= failures.size()) {
                        return CompletableFuture.failedFuture(failures.get(attemptedPaths.size() - 1));
                    }
                    return store.delete(path, invocation.getArgument(1));
                }).when(observed).delete(anyString(), any());

                assertThatThrownBy(owner::close).isInstanceOf(UnavailableException.class)
                        .satisfies(error -> {
                            assertThat(error.getCause()).isInstanceOf(ExecutionException.class)
                                    .hasCause(failures.get(0));
                            assertThat(error.getSuppressed()).hasSize(1);
                            assertThat(error.getSuppressed()[0]).isInstanceOf(ExecutionException.class)
                                    .hasCause(failures.get(1));
                        });
                assertThat(attemptedPaths).hasSize(LEDGERS.size());
                for (long ledgerId : LEDGERS) {
                    assertThat(other.isLedgerBeingReplicated(ledgerId))
                            .isEqualTo(attemptedPaths.subList(0, 2).contains(lockPath(root, ledgerId)));
                }
                long reacquired = other.pollLedgerToRereplicate();
                assertThat(reacquired).isIn(LEDGERS);
                List<String> retryPaths = new ArrayList<>();
                doAnswer(invocation -> {
                    String path = invocation.getArgument(0);
                    retryPaths.add(path);
                    return store.delete(path, invocation.getArgument(1));
                }).when(observed).delete(anyString(), any());

                owner.close();

                assertThat(retryPaths).containsExactlyInAnyOrderElementsOf(attemptedPaths.subList(0, 2));
                assertThat(other.isLedgerBeingReplicated(reacquired)).isTrue();
            }
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testCloseContinuesAfterTimeout(String provider, Supplier<String> urlSupplier) throws Exception {
        String root = "/ledgers-" + UUID.randomUUID();
        try (var store = newStore(urlSupplier)) {
            var observed = spy(store);
            try (var owner = newManager(observed, root);
                 var other = newManager(store, root)) {
                acquireLedgers(owner);
                CompletableFuture<Void> pendingDelete = new CompletableFuture<>();
                MetadataStoreException immediateFailure = new MetadataStoreException("immediate failure");
                List<CompletableFuture<Void>> completedDeletes = new ArrayList<>();
                List<String> attemptedPaths = new ArrayList<>();
                doAnswer(invocation -> {
                    String path = invocation.getArgument(0);
                    attemptedPaths.add(path);
                    if (attemptedPaths.size() == 1) {
                        return pendingDelete;
                    }
                    if (attemptedPaths.size() == 2) {
                        return CompletableFuture.failedFuture(immediateFailure);
                    }
                    CompletableFuture<Void> delete = store.delete(path, invocation.getArgument(1));
                    completedDeletes.add(delete);
                    return delete;
                }).when(observed).delete(anyString(), any());

                assertThatThrownBy(() -> owner.close(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(UnavailableException.class)
                        .hasCauseInstanceOf(TimeoutException.class)
                        .satisfies(error -> {
                            assertThat(error.getSuppressed()).hasSize(1);
                            assertThat(error.getSuppressed()[0]).hasRootCause(immediateFailure);
                        });
                FutureUtil.waitForAll(completedDeletes).get(5, TimeUnit.SECONDS);
                assertThat(attemptedPaths).hasSize(LEDGERS.size());
                for (long ledgerId : LEDGERS) {
                    assertThat(other.isLedgerBeingReplicated(ledgerId))
                            .isEqualTo(attemptedPaths.subList(0, 2).contains(lockPath(root, ledgerId)));
                }

                pendingDelete.completeExceptionally(new MetadataStoreException("delete failed"));
                List<String> retryPaths = new ArrayList<>();
                doAnswer(invocation -> {
                    retryPaths.add(invocation.getArgument(0));
                    return store.delete(invocation.getArgument(0), invocation.getArgument(1));
                })
                        .when(observed).delete(anyString(), any());
                owner.close();

                assertThat(retryPaths).containsExactlyInAnyOrderElementsOf(attemptedPaths.subList(0, 2));
                assertAllUnlocked(other);
            }
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testCloseUsesSingleOverallTimeout(String provider, Supplier<String> urlSupplier) throws Exception {
        String root = "/ledgers-" + UUID.randomUUID();
        int lockCount = 20;
        try (var store = newStore(urlSupplier)) {
            var observed = spy(store);
            try (var owner = newManager(observed, root)) {
                for (long ledgerId = 1; ledgerId <= lockCount; ledgerId++) {
                    owner.acquireUnderreplicatedLedger(ledgerId);
                }
                AtomicInteger attempts = new AtomicInteger();
                List<CompletableFuture<Void>> pendingDeletes = new ArrayList<>();
                doAnswer(invocation -> {
                    attempts.incrementAndGet();
                    CompletableFuture<Void> pendingDelete = new CompletableFuture<>();
                    pendingDeletes.add(pendingDelete);
                    return pendingDelete;
                }).when(observed).delete(anyString(), any());

                long startNanos = System.nanoTime();
                try {
                    assertThatThrownBy(() -> owner.close(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(UnavailableException.class)
                            .hasCauseInstanceOf(TimeoutException.class);
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    assertThat(attempts.get()).isEqualTo(lockCount);
                    assertThat(elapsedMillis).isLessThan(1000L);
                } finally {
                    pendingDeletes.forEach(delete -> delete.completeExceptionally(
                            new MetadataStoreException("delete failed")));
                    doAnswer(invocation -> store.delete(invocation.getArgument(0), invocation.getArgument(1)))
                            .when(observed).delete(anyString(), any());
                    owner.close();
                }
            }
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testLateSuccessfulDeleteIsNotRetriedAfterTimeout(
            String provider, Supplier<String> urlSupplier) throws Exception {
        String root = "/ledgers-" + UUID.randomUUID();
        long ledgerId = 1L;
        try (var store = newStore(urlSupplier)) {
            var observed = spy(store);
            try (var owner = newManager(observed, root);
                 var other = newManager(store, root)) {
                owner.acquireUnderreplicatedLedger(ledgerId);
                CompletableFuture<Void> delayedAcknowledgement = new CompletableFuture<>();
                AtomicInteger attempts = new AtomicInteger();
                doAnswer(invocation -> {
                    attempts.incrementAndGet();
                    store.delete(invocation.getArgument(0), invocation.getArgument(1)).get(5, TimeUnit.SECONDS);
                    return delayedAcknowledgement;
                }).when(observed).delete(anyString(), any());

                assertThatThrownBy(() -> owner.close(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(UnavailableException.class)
                        .hasCauseInstanceOf(TimeoutException.class);
                other.acquireUnderreplicatedLedger(ledgerId);
                delayedAcknowledgement.complete(null);

                owner.close();

                assertThat(attempts.get()).isEqualTo(1);
                assertThat(other.isLedgerBeingReplicated(ledgerId)).isTrue();
            }
        }
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void testClosePreservesInterruptAndPreviousFailure(String provider, Supplier<String> urlSupplier)
            throws Exception {
        String root = "/ledgers-" + UUID.randomUUID();
        try (var store = newStore(urlSupplier)) {
            var observed = spy(store);
            try (var owner = newManager(observed, root);
                 var other = newManager(store, root)) {
                acquireLedgers(owner);
                MetadataStoreException firstFailure = new MetadataStoreException("first failure");
                AtomicInteger attempts = new AtomicInteger();
                CountDownLatch allDeletesSubmitted = new CountDownLatch(LEDGERS.size());
                List<CompletableFuture<Void>> pendingDeletes = new ArrayList<>();
                doAnswer(invocation -> {
                    int attempt = attempts.getAndIncrement();
                    CompletableFuture<Void> result;
                    if (attempt == 0) {
                        result = CompletableFuture.failedFuture(firstFailure);
                    } else {
                        result = new CompletableFuture<>();
                        pendingDeletes.add(result);
                    }
                    allDeletesSubmitted.countDown();
                    return result;
                }).when(observed).delete(anyString(), any());
                AtomicReference<Throwable> closeFailure = new AtomicReference<>();
                AtomicBoolean interruptPreserved = new AtomicBoolean();
                Thread closeThread = new Thread(() -> {
                    try {
                        owner.close();
                    } catch (UnavailableException error) {
                        closeFailure.set(error);
                        interruptPreserved.set(Thread.currentThread().isInterrupted());
                    }
                });
                try {
                    closeThread.start();
                    assertThat(allDeletesSubmitted.await(5, TimeUnit.SECONDS)).isTrue();
                    closeThread.interrupt();
                    closeThread.join(5000);
                    assertThat(closeThread.isAlive()).isFalse();
                    assertThat(closeFailure.get()).isInstanceOf(UnavailableException.class)
                            .hasCauseInstanceOf(InterruptedException.class)
                            .satisfies(error -> {
                                assertThat(error.getSuppressed()).hasSize(1);
                                assertThat(error.getSuppressed()[0]).isInstanceOf(UnavailableException.class)
                                        .hasRootCause(firstFailure);
                            });
                    assertThat(interruptPreserved.get()).isTrue();
                    assertThat(attempts.get()).isEqualTo(LEDGERS.size());
                } finally {
                    if (closeThread.isAlive()) {
                        closeThread.interrupt();
                        closeThread.join(5000);
                    }
                    pendingDeletes.forEach(delete -> delete.completeExceptionally(
                            new MetadataStoreException("delete failed")));
                    doAnswer(invocation -> store.delete(invocation.getArgument(0), invocation.getArgument(1)))
                            .when(observed).delete(anyString(), any());
                }
                assertAllLocked(other);
                owner.close();
                assertAllUnlocked(other);
            }
        }
    }

    private static MetadataStoreExtended newStore(Supplier<String> urlSupplier) throws Exception {
        return MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
    }

    private static PulsarLedgerUnderreplicationManager newManager(MetadataStoreExtended store, String root)
            throws Exception {
        return new PulsarLedgerUnderreplicationManager(new ClientConfiguration(), store, root);
    }

    private static String lockPath(String root, long ledgerId) {
        return PulsarLedgerUnderreplicationManager.getUrLedgerLockPath(
                PulsarLedgerUnderreplicationManager.getUrLockPath(root), ledgerId);
    }

    private static void acquireLedgers(PulsarLedgerUnderreplicationManager manager) throws Exception {
        for (long ledgerId : LEDGERS) {
            manager.markLedgerUnderreplicated(ledgerId, "bookie:3181");
        }
        acquireExistingLedgers(manager);
    }

    private static void acquireExistingLedgers(PulsarLedgerUnderreplicationManager manager) throws Exception {
        List<Long> acquired = new ArrayList<>();
        for (int i = 0; i < LEDGERS.size(); i++) {
            acquired.add(manager.pollLedgerToRereplicate());
        }
        assertThat(acquired).containsExactlyInAnyOrderElementsOf(LEDGERS);
    }

    private static void assertAllLocked(PulsarLedgerUnderreplicationManager manager) throws Exception {
        for (long ledgerId : LEDGERS) {
            assertThat(manager.isLedgerBeingReplicated(ledgerId)).as("ledger %s remains locked", ledgerId).isTrue();
        }
    }

    private static void assertAllUnlocked(PulsarLedgerUnderreplicationManager manager) throws Exception {
        for (long ledgerId : LEDGERS) {
            assertThat(manager.isLedgerBeingReplicated(ledgerId)).as("ledger %s is unlocked", ledgerId).isFalse();
        }
    }
}
