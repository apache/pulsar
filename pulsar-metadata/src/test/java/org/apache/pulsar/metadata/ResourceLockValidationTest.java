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
package org.apache.pulsar.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.testng.annotations.Test;

/** Tests the explicit validation contract against the embedded ZooKeeper's actual session ownership. */
public class ResourceLockValidationTest extends BaseMetadataStoreTest {

    @Test(timeOut = 30000)
    public void validatesOwnLeaseWithoutWritingAndPreservesLegitimateValueUpdates() throws Exception {
        @Cleanup
        MetadataStoreExtended store = newStore();
        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);
        ResourceLock<String> lock = coordinationService.getLockManager(String.class)
                .acquireLock(newKey(), "owner-a").get(5, TimeUnit.SECONDS);
        GetResult original = read(store, lock.getPath());
        assertThat(original.getStat().isEphemeral()).isTrue();
        assertThat(original.getStat().isCreatedBySelf()).isTrue();

        lock.validate().get(5, TimeUnit.SECONDS);
        assertUnchanged(store, lock.getPath(), original);
        assertThat(lock.getLockExpiredFuture()).isNotDone();

        lock.updateValue("owner-a-updated").get(5, TimeUnit.SECONDS);
        GetResult updated = read(store, lock.getPath());
        assertThat(updated.getValue()).isEqualTo("\"owner-a-updated\"".getBytes(UTF_8));
        assertThat(lock.getValue()).isEqualTo("owner-a-updated");
        lock.validate().get(5, TimeUnit.SECONDS);
        assertUnchanged(store, lock.getPath(), updated);
        assertThat(lock.getLockExpiredFuture()).isNotDone();
    }

    @Test(timeOut = 30000)
    public void missingLeaseIsRestoredWithoutDeliveringAnyQueuedNotification() throws Exception {
        @Cleanup
        MetadataStoreExtended store = newStore();
        NotificationGate notifications = new NotificationGate();
        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(notifications.wrap(store));
        String path = newKey();
        try {
            ResourceLock<String> lock = coordinationService.getLockManager(String.class)
                    .acquireLock(path, "owner-a").get(5, TimeUnit.SECONDS);
            GetResult original = read(store, path);
            store.delete(path, Optional.of(original.getStat().getVersion())).get(5, TimeUnit.SECONDS);
            notifications.awaitCount(path, NotificationType.Deleted, 1);
            assertThat(store.get(path).get(5, TimeUnit.SECONDS)).isEmpty();

            lock.validate().get(5, TimeUnit.SECONDS);

            GetResult restored = read(store, path);
            assertThat(restored.getValue()).isEqualTo(original.getValue());
            assertThat(restored.getStat().isEphemeral()).isTrue();
            assertThat(restored.getStat().isCreatedBySelf()).isTrue();
            assertThat(restored.getStat().getVersion()).isZero();
            assertThat(lock.getLockExpiredFuture()).isNotDone();
            notifications.awaitCount(path, NotificationType.Created, 2);
            assertThat(notifications.typesFor(path)).containsExactly(
                    NotificationType.Created, NotificationType.Deleted, NotificationType.Created);
            assertThat(notifications.deliveredCount()).as("validation must not need queued callbacks").isZero();
        } finally {
            notifications.release();
        }
    }

    @Test(timeOut = 30000)
    public void foreignValidationExpiresBeforeQueuedUpdateAndRelease() throws Exception {
        @Cleanup
        MetadataStoreExtended storeA = newStore();
        @Cleanup
        MetadataStoreExtended storeB = newStore();
        NotificationGate notifications = new NotificationGate();
        MetadataStoreExtended observedA = notifications.wrap(storeA);
        @Cleanup
        CoordinationService coordinationServiceA = new CoordinationServiceImpl(observedA);
        @Cleanup
        CoordinationService coordinationServiceB = new CoordinationServiceImpl(storeB);
        LockManager<String> managerA = coordinationServiceA.getLockManager(String.class);
        LockManager<String> managerB = coordinationServiceB.getLockManager(String.class);
        CompletableFuture<Void> returnRead = new CompletableFuture<>();
        String path = newKey();
        try {
            ResourceLock<String> lockA = managerA.acquireLock(path, "owner-a").get(5, TimeUnit.SECONDS);
            GetResult ownerA = read(storeA, path);
            storeB.delete(path, Optional.empty()).get(5, TimeUnit.SECONDS);
            ResourceLock<String> lockB = managerB.acquireLock(path, "owner-b").get(5, TimeUnit.SECONDS);
            GetResult ownerB = read(storeB, path);
            assertThat(ownerB.getStat().isCreatedBySelf()).isTrue();
            assertThat(ownerB.getStat().getVersion()).as("the new lease reuses A's old ZooKeeper data version")
                    .isEqualTo(ownerA.getStat().getVersion());
            notifications.awaitCount(path, NotificationType.Created, 2);
            assertThat(notifications.typesFor(path)).containsExactly(
                    NotificationType.Created, NotificationType.Deleted, NotificationType.Created);

            CompletableFuture<Optional<GetResult>> capturedRead = new CompletableFuture<>();
            doAnswer(invocation -> storeA.get(path).thenCompose(actual -> {
                capturedRead.complete(actual);
                return returnRead.thenApply(__ -> actual);
            })).when(observedA).get(path);

            CompletableFuture<Void> validation = lockA.validate();
            GetResult foreign = capturedRead.get(5, TimeUnit.SECONDS).orElseThrow();
            assertThat(foreign.getValue()).isEqualTo(ownerB.getValue());
            assertThat(foreign.getStat().isEphemeral()).isTrue();
            assertThat(foreign.getStat().isCreatedBySelf()).isFalse();
            CompletableFuture<Void> update = lockA.updateValue("owner-a-updated");
            CompletableFuture<Void> release = lockA.release().thenRun(() ->
                    assertThat(lockA.getLockExpiredFuture()).as("expiry precedes queue handoff").isDone());
            assertThat(validation).isNotDone();
            assertThat(update).isNotDone();
            assertThat(release).isNotDone();
            assertThat(lockA.getLockExpiredFuture()).isNotDone();
            verify(observedA, never()).delete(eq(path), any());
            verify(observedA).put(eq(path), any(), any(), eq(EnumSet.of(CreateOption.Ephemeral)));

            returnRead.complete(null);
            assertThatThrownBy(() -> validation.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(LockBusyException.class);
            assertThatThrownBy(() -> update.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            release.get(5, TimeUnit.SECONDS);

            verify(observedA, never()).delete(eq(path), any());
            verify(observedA).put(eq(path), any(), any(), eq(EnumSet.of(CreateOption.Ephemeral)));
            assertThat(lockB.getLockExpiredFuture()).isNotDone();
            assertUnchanged(storeB, path, ownerB);
            assertThat(notifications.deliveredCount()).as("all A callbacks remain queued in FIFO order").isZero();
        } finally {
            returnRead.complete(null);
            notifications.release();
        }
    }

    @Test(timeOut = 30000)
    public void unavailableOrUnknownReadFailsWithoutExpiringHealthyLocksAndCanBeRetried() throws Exception {
        @Cleanup
        MetadataStoreExtended store = newStore();
        MetadataStoreExtended observed = mock(MetadataStoreExtended.class, delegatesTo(store));
        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(observed);
        LockManager<String> manager = coordinationService.getLockManager(String.class);
        ResourceLock<String> lock = manager.acquireLock(newKey(), "owner-a").get(5, TimeUnit.SECONDS);
        ResourceLock<String> healthy = manager.acquireLock(newKey(), "healthy").get(5, TimeUnit.SECONDS);
        GetResult original = read(store, lock.getPath());
        GetResult other = read(store, healthy.getPath());

        for (Throwable failure : List.of(new MetadataStoreException("metadata read unavailable"),
                new IllegalStateException("unknown read failure"))) {
            doReturn(CompletableFuture.failedFuture(failure)).when(observed).get(lock.getPath());
            CompletableFuture<Void> validation = lock.validate();
            assertThatThrownBy(() -> validation.get(5, TimeUnit.SECONDS)).hasCause(failure);
            assertThat(lock.getLockExpiredFuture()).isNotDone();
            healthy.validate().get(5, TimeUnit.SECONDS);
            assertThat(healthy.getLockExpiredFuture()).isNotDone();
            assertUnchanged(store, lock.getPath(), original);
            assertUnchanged(store, healthy.getPath(), other);
        }

        doAnswer(invocation -> store.get(lock.getPath())).when(observed).get(lock.getPath());
        lock.validate().get(5, TimeUnit.SECONDS);
        assertThat(lock.getLockExpiredFuture()).isNotDone();
        assertUnchanged(store, lock.getPath(), original);
        assertUnchanged(store, healthy.getPath(), other);
    }

    @Test(timeOut = 30000)
    public void validateWaitsForSyncAndSkipsReadWhenSyncFails() throws Exception {
        @Cleanup
        MetadataStoreExtended store = newStore();
        MetadataStoreExtended observed = mock(MetadataStoreExtended.class, delegatesTo(store));
        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(observed);
        ResourceLock<String> lock = coordinationService.getLockManager(String.class)
                .acquireLock(newKey(), "owner-a").get(5, TimeUnit.SECONDS);
        String path = lock.getPath();
        GetResult original = read(store, path);
        CompletableFuture<Void> actualSyncCompleted = new CompletableFuture<>();
        CompletableFuture<Void> allowSyncCompletion = new CompletableFuture<>();
        doAnswer(invocation -> store.sync(path).thenCompose(__ -> {
            actualSyncCompleted.complete(null);
            return allowSyncCompletion;
        })).when(observed).sync(path);
        try {
            CompletableFuture<Void> validation = lock.validate();
            actualSyncCompleted.get(5, TimeUnit.SECONDS);
            assertThat(validation).isNotDone();
            verify(observed, never()).get(path);

            allowSyncCompletion.complete(null);
            validation.get(5, TimeUnit.SECONDS);
            verify(observed).get(path);
            assertThat(lock.getLockExpiredFuture()).isNotDone();
            assertUnchanged(store, path, original);

            MetadataStoreException failure = new MetadataStoreException("metadata sync unavailable");
            doReturn(CompletableFuture.failedFuture(failure)).when(observed).sync(path);
            CompletableFuture<Void> failedValidation = lock.validate();
            assertThatThrownBy(() -> failedValidation.get(5, TimeUnit.SECONDS)).hasCause(failure);
            // Exactly the first validation read; the failed sync must not start another read.
            verify(observed).get(path);
            assertThat(lock.getLockExpiredFuture()).isNotDone();
            assertUnchanged(store, path, original);
        } finally {
            allowSyncCompletion.complete(null);
        }
    }

    @Test(timeOut = 10000)
    @SuppressWarnings("unchecked")
    public void legacyImplementationReturnsAnUnsupportedFuture() {
        ResourceLock<String> legacy = mock(ResourceLock.class, CALLS_REAL_METHODS);
        CompletableFuture<Void> validation = legacy.validate();
        assertThatThrownBy(() -> validation.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    private MetadataStoreExtended newStore() throws Exception {
        // No provider matrix: this contract requires real distinct sessions and must not start an Oxia image.
        return MetadataStoreExtended.create(zks.getConnectionString(), MetadataStoreConfig.builder().build());
    }

    private static GetResult read(MetadataStoreExtended store, String path) throws Exception {
        return store.get(path).get(5, TimeUnit.SECONDS).orElseThrow();
    }

    private static void assertUnchanged(MetadataStoreExtended store, String path, GetResult expected) throws Exception {
        GetResult actual = read(store, path);
        assertThat(actual.getValue()).isEqualTo(expected.getValue());
        assertThat(actual.getStat()).isEqualTo(expected.getStat());
    }

    /** Delays A's actual coordination callbacks, including every notification type and parent path, in FIFO order. */
    private static final class NotificationGate {
        private final Queue<Runnable> pending = new ArrayDeque<>();
        private final List<Notification> received = new CopyOnWriteArrayList<>();
        private boolean released;
        private int delivered;

        MetadataStoreExtended wrap(MetadataStoreExtended store) {
            MetadataStoreExtended observed = mock(MetadataStoreExtended.class, delegatesTo(store));
            doAnswer(invocation -> {
                Consumer<Notification> listener = invocation.getArgument(0);
                return store.registerCancellableListener(notification -> accept(listener, notification));
            }).when(observed).registerCancellableListener(any());
            return observed;
        }

        private synchronized void accept(Consumer<Notification> listener, Notification notification) {
            if (released) {
                listener.accept(notification);
                delivered++;
            } else {
                pending.add(() -> {
                    listener.accept(notification);
                    delivered++;
                });
                received.add(notification);
            }
        }

        void awaitCount(String path, NotificationType type, long count) {
            await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> typesFor(path).stream().filter(type::equals).count() >= count);
        }

        List<NotificationType> typesFor(String path) {
            return received.stream().filter(notification -> path.equals(notification.getPath()))
                    .map(Notification::getType).toList();
        }

        synchronized int deliveredCount() {
            return delivered;
        }

        synchronized void release() {
            released = true;
            while (!pending.isEmpty()) {
                pending.remove().run();
            }
        }
    }
}
