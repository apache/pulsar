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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.NotificationType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarLedgerMetadataListenerTest {
    private static final long LEDGER_ID = 1L;
    private static final List<BookieId> UPDATED_ENSEMBLE = List.of(BookieId.parse("bookie-2:3181"));

    private MetadataStore store;
    private PulsarLedgerManager manager;
    private Versioned<LedgerMetadata> initialMetadata;
    private AtomicReference<Thread> deletionThread;

    @BeforeMethod
    public void setup() throws Exception {
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
        deletionThread = new AtomicReference<>();
        store.registerListener(notification -> {
            if (notification.getType() == NotificationType.Deleted) {
                deletionThread.set(Thread.currentThread());
            }
        });
        manager = new PulsarLedgerManager(store, "/ledgers");
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(LEDGER_ID)
                .withEnsembleSize(1).withWriteQuorumSize(1).withAckQuorumSize(1)
                .withPassword(new byte[0])
                .withDigestType(DigestType.CRC32C)
                .newEnsembleEntry(0L, List.of(BookieId.parse("bookie-1:3181")))
                .build();
        initialMetadata = manager.createLedgerMetadata(LEDGER_ID, metadata).get(10, TimeUnit.SECONDS);
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
    public Object[][] replacementListenerSet() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "replacementListenerSet", timeOut = 30000)
    public void testRegistrationRacingWithLastListenerRemoval(boolean registerReplacement) throws Exception {
        LedgerMetadataListener oldListener = mock(LedgerMetadataListener.class);
        LedgerMetadataListener newListener = mock(LedgerMetadataListener.class);
        LedgerMetadataListener replacementListener = mock(LedgerMetadataListener.class);
        manager.registerLedgerMetadataListener(LEDGER_ID, oldListener);
        verifyMetadataNotification(oldListener, initialMetadata);
        Set<LedgerMetadataListener> originalSet = manager.listeners.get(LEDGER_ID);

        ExecutorService executor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("ledger-listener-registration-test"));
        AtomicReference<Thread> registrationThread = new AtomicReference<>();
        try {
            Future<?> registration;
            synchronized (originalSet) {
                registration = executor.submit(() -> {
                    registrationThread.set(Thread.currentThread());
                    manager.registerLedgerMetadataListener(LEDGER_ID, newListener);
                });
                // Wait until registration has obtained the old set and is blocked acquiring its monitor.
                // No listener state is injected: only the ordering of the public operations is controlled.
                awaitBlockedOn(registrationThread, originalSet);

                manager.unregisterLedgerMetadataListener(LEDGER_ID, oldListener);
                assertThat(manager.listeners).doesNotContainKey(LEDGER_ID);
                if (registerReplacement) {
                    manager.registerLedgerMetadataListener(LEDGER_ID, replacementListener);
                }
            }
            registration.get(10, TimeUnit.SECONDS);

            Versioned<LedgerMetadata> updated = updateEnsemble();
            verifyMetadataNotification(newListener, updated);
            if (registerReplacement) {
                verifyMetadataNotification(replacementListener, updated);
            }

            manager.unregisterLedgerMetadataListener(LEDGER_ID, newListener);
            manager.unregisterLedgerMetadataListener(LEDGER_ID, replacementListener);
            assertThat(manager.listeners).isEmpty();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @DataProvider
    public Object[][] staleRemoval() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "staleRemoval", timeOut = 30000)
    public void testStaleRemovalDoesNotRemoveNewListenerSet(boolean ledgerDeleted) throws Exception {
        LedgerMetadataListener oldListener = mock(LedgerMetadataListener.class);
        LedgerMetadataListener notifications = mock(LedgerMetadataListener.class);
        manager.registerLedgerMetadataListener(LEDGER_ID, oldListener);
        verifyMetadataNotification(oldListener, initialMetadata);
        Set<LedgerMetadataListener> originalSet = manager.listeners.get(LEDGER_ID);

        CompletableFuture<Void> addingListener = new CompletableFuture<>();
        CountDownLatch continueRegistration = new CountDownLatch(1);
        AtomicReference<Thread> registrationThread = new AtomicReference<>();
        LedgerMetadataListener newListener = new LedgerMetadataListener() {
            @Override
            public void onChanged(long ledgerId, Versioned<LedgerMetadata> metadata) {
                notifications.onChanged(ledgerId, metadata);
            }

            @Override
            public int hashCode() {
                Set<LedgerMetadataListener> currentSet = manager.listeners.get(LEDGER_ID);
                if (Thread.currentThread() == registrationThread.get()
                        && currentSet != null && Thread.holdsLock(currentSet)) {
                    // Pause HashSet.add after the registration's identity check but before insertion.
                    addingListener.complete(null);
                    try {
                        assertThat(continueRegistration.await(10, TimeUnit.SECONDS))
                                .as("Registration should be resumed after the stale removal").isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
                return System.identityHashCode(this);
            }

            @Override
            public boolean equals(Object other) {
                return this == other;
            }

            @Override
            public String toString() {
                return "listener-with-controlled-registration";
            }
        };

        CompletableFuture<Void> deletionProcessed = new CompletableFuture<>();
        store.registerListener(notification -> {
            if (notification.getType() == NotificationType.Deleted
                    && notification.getPath().equals(manager.getLedgerPath(LEDGER_ID))) {
                deletionProcessed.complete(null);
            }
        });
        ExecutorService executor = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("ledger-listener-stale-removal-test"));
        AtomicReference<Thread> unregistrationThread = new AtomicReference<>();
        try {
            Future<?> staleRemoval;
            Future<?> registration;
            synchronized (originalSet) {
                if (ledgerDeleted) {
                    store.delete(manager.getLedgerPath(LEDGER_ID), Optional.empty()).get(10, TimeUnit.SECONDS);
                    awaitBlockedOn(deletionThread, originalSet);
                    staleRemoval = deletionProcessed;
                } else {
                    staleRemoval = executor.submit(() -> {
                        unregistrationThread.set(Thread.currentThread());
                        manager.unregisterLedgerMetadataListener(LEDGER_ID, oldListener);
                    });
                    awaitBlockedOn(unregistrationThread, originalSet);
                }

                manager.unregisterLedgerMetadataListener(LEDGER_ID, oldListener);
                assertThat(manager.listeners).doesNotContainKey(LEDGER_ID);
                registration = executor.submit(() -> {
                    registrationThread.set(Thread.currentThread());
                    manager.registerLedgerMetadataListener(LEDGER_ID, newListener);
                });
                addingListener.get(10, TimeUnit.SECONDS);
                assertThat(manager.listeners.get(LEDGER_ID)).isNotSameAs(originalSet).isEmpty();
            }
            // The stale operation now runs while the new, locked set is still empty.
            staleRemoval.get(10, TimeUnit.SECONDS);
            continueRegistration.countDown();
            registration.get(10, TimeUnit.SECONDS);

            if (ledgerDeleted) {
                verify(notifications, timeout(10000)).onChanged(LEDGER_ID, null);
            } else {
                verifyMetadataNotification(notifications, updateEnsemble());
                manager.unregisterLedgerMetadataListener(LEDGER_ID, newListener);
            }
            assertThat(manager.listeners).isEmpty();
        } finally {
            continueRegistration.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(timeOut = 30000)
    public void testDeleteNotifiesAndRemovesListeners() throws Exception {
        LedgerMetadataListener first = mock(LedgerMetadataListener.class);
        LedgerMetadataListener second = mock(LedgerMetadataListener.class);
        manager.registerLedgerMetadataListener(LEDGER_ID, first);
        manager.registerLedgerMetadataListener(LEDGER_ID, second);
        verifyMetadataNotification(first, initialMetadata);
        verifyMetadataNotification(second, initialMetadata);

        store.delete(manager.getLedgerPath(LEDGER_ID), Optional.empty()).get(10, TimeUnit.SECONDS);

        verify(first, timeout(10000)).onChanged(LEDGER_ID, null);
        verify(second, timeout(10000)).onChanged(LEDGER_ID, null);
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(manager.listeners).isEmpty());
    }

    @Test(timeOut = 30000)
    public void testRegisterListenerForMissingLedger() {
        LedgerMetadataListener listener = mock(LedgerMetadataListener.class);
        manager.registerLedgerMetadataListener(LEDGER_ID + 1, listener);

        verify(listener, timeout(10000)).onChanged(LEDGER_ID + 1, null);
        assertThat(manager.listeners).isEmpty();
    }

    @Test(timeOut = 30000)
    public void testRemoveLedgerMetadataClearsListeners() throws Exception {
        LedgerMetadataListener listener = mock(LedgerMetadataListener.class);
        manager.registerLedgerMetadataListener(LEDGER_ID, listener);
        verifyMetadataNotification(listener, initialMetadata);

        manager.removeLedgerMetadata(LEDGER_ID, Version.ANY).get(10, TimeUnit.SECONDS);

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(manager.listeners).isEmpty());
    }

    @Test(timeOut = 30000)
    public void testUnregisterKeepsOtherListeners() throws Exception {
        LedgerMetadataListener first = mock(LedgerMetadataListener.class);
        LedgerMetadataListener second = mock(LedgerMetadataListener.class);
        manager.registerLedgerMetadataListener(LEDGER_ID, first);
        manager.registerLedgerMetadataListener(LEDGER_ID, second);
        manager.unregisterLedgerMetadataListener(LEDGER_ID, first);

        verifyMetadataNotification(second, updateEnsemble());
        assertThat(manager.listeners.get(LEDGER_ID)).containsExactly(second);

        manager.unregisterLedgerMetadataListener(LEDGER_ID, second);
        assertThat(manager.listeners).isEmpty();
    }

    @Test(timeOut = 30000)
    public void testDuplicateAndNullRegistration() {
        manager.registerLedgerMetadataListener(LEDGER_ID, null);
        assertThat(manager.listeners).isEmpty();

        LedgerMetadataListener listener = mock(LedgerMetadataListener.class);
        manager.registerLedgerMetadataListener(LEDGER_ID, listener);
        manager.registerLedgerMetadataListener(LEDGER_ID, listener);
        assertThat(manager.listeners.get(LEDGER_ID)).containsExactly(listener);

        manager.unregisterLedgerMetadataListener(LEDGER_ID, listener);
        manager.unregisterLedgerMetadataListener(LEDGER_ID, listener);
        assertThat(manager.listeners).isEmpty();
    }

    private static void awaitBlockedOn(AtomicReference<Thread> threadReference, Object monitor) {
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            Thread thread = threadReference.get();
            if (thread == null) {
                return false;
            }
            ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
            return info != null && info.getThreadState() == Thread.State.BLOCKED
                    && info.getLockInfo() != null
                    && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(monitor);
        });
    }

    private Versioned<LedgerMetadata> updateEnsemble() throws Exception {
        LedgerMetadata updated = LedgerMetadataBuilder.from(initialMetadata.getValue())
                .replaceEnsembleEntry(0L, UPDATED_ENSEMBLE).build();
        return manager.writeLedgerMetadata(LEDGER_ID, updated, initialMetadata.getVersion())
                .get(10, TimeUnit.SECONDS);
    }

    private static void verifyMetadataNotification(LedgerMetadataListener listener,
                                                   Versioned<LedgerMetadata> expected) {
        verify(listener, timeout(10000).atLeastOnce()).onChanged(eq(LEDGER_ID), argThat(metadata -> metadata != null
                && metadata.getVersion().equals(expected.getVersion())
                && metadata.getValue().getAllEnsembles().equals(expected.getValue().getAllEnsembles())));
    }
}
