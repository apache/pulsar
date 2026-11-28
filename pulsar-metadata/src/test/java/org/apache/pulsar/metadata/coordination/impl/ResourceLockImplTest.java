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
package org.apache.pulsar.metadata.coordination.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.testng.annotations.Test;

/** Resource-lock lifecycle after local ownership-cache loss, without an external metadata service. */
public class ResourceLockImplTest {
    private static final String PATH = "/locks/reacquired";
    private static final String OWNER = "broker-self";
    private static final MetadataSerde<String> SERDE = new MetadataSerde<>() {
        @Override
        public byte[] serialize(String path, String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(String path, byte[] content, Stat stat) {
            return new String(content, StandardCharsets.UTF_8);
        }
    };

    @Test(timeOut = 10000)
    public void freshSameSessionHandleCanUpdateValue() throws Exception {
        try (var store = new FaultInjectionMetadataStore(newStore()) {
            @Override
            public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> version, Set<Option> opts) {
                // A wrapped version conflict must still make the fresh handle revalidate the existing lock.
                return super.put(path, value, version, opts)
                        .exceptionallyCompose(error -> CompletableFuture.failedFuture(new ExecutionException(error)));
            }
        }) {
            Stat original = store.put(PATH, SERDE.serialize(PATH, OWNER), Optional.of(-1L),
                    EnumSet.of(CreateOption.Ephemeral)).get(5, TimeUnit.SECONDS);
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(store, SERDE, PATH,
                    mock(ScheduledExecutorService.class));
            lock.acquire(OWNER).get(5, TimeUnit.SECONDS);

            lock.updateValue("updated-owner").get(5, TimeUnit.SECONDS);
            assertThat(lock.getValue()).isEqualTo("updated-owner");
            GetResult current = store.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow();
            assertThat(current.getStat().getVersion()).isGreaterThan(original.getVersion());
            assertThat(SERDE.deserialize(PATH, current.getValue(), current.getStat())).isEqualTo("updated-owner");
            assertThat(lock.getLockExpiredFuture()).isNotDone();
            lock.release().get(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeOut = 10000)
    public void freshSameSessionHandleExpiresWhenDeletedNotificationFindsForeignOwner() throws Exception {
        try (MetadataStoreExtended backing = newStore()) {
            Stat original = backing.put(PATH, SERDE.serialize(PATH, OWNER), Optional.of(-1L),
                    EnumSet.of(CreateOption.Ephemeral)).get(5, TimeUnit.SECONDS);
            MetadataStoreExtended store = spy(backing);
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(store, SERDE, PATH,
                    mock(ScheduledExecutorService.class));
            lock.acquire(OWNER).get(5, TimeUnit.SECONDS);
            backing.delete(PATH, Optional.of(original.getVersion())).get(5, TimeUnit.SECONDS);
            Stat foreign = backing.put(PATH, SERDE.serialize(PATH, "foreign-owner"), Optional.of(-1L),
                    EnumSet.of(CreateOption.Ephemeral)).get(5, TimeUnit.SECONDS);
            // LocalMemory has only one session. Supply the legitimate external observation that
            // the deletion notification now finds a node created by a different session.
            Stat foreignSession = new Stat(PATH, foreign.getVersion(), foreign.getCreationTimestamp(),
                    foreign.getModificationTimestamp(), true, false);
            doReturn(CompletableFuture.completedFuture(Optional.of(
                    new GetResult(SERDE.serialize(PATH, "foreign-owner"), foreignSession))))
                    .when(store).get(PATH);

            lock.lockWasInvalidated();
            lock.getLockExpiredFuture().get(5, TimeUnit.SECONDS);
            assertThat(failure(lock.updateValue("must-not-revive"))).isInstanceOf(IllegalStateException.class);
            verify(store, never()).delete(eq(PATH), any());
            GetResult retained = backing.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow();
            assertThat(retained.getStat().getVersion()).isEqualTo(foreign.getVersion());
            assertThat(SERDE.deserialize(PATH, retained.getValue(), retained.getStat())).isEqualTo("foreign-owner");
            lock.release().get(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeOut = 10000)
    public void delayedSameSessionReadCannotReviveReleasingHandle() throws Exception {
        try (MetadataStoreExtended backing = newStore()) {
            MetadataStoreExtended store = spy(backing);
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(store, SERDE, PATH,
                    mock(ScheduledExecutorService.class));
            lock.acquire(OWNER).get(5, TimeUnit.SECONDS);
            Stat original = backing.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow().getStat();
            // Make the public update encounter a version conflict and re-read the same session's
            // value. Delay that read's response until release has begun.
            Stat newer = backing.put(PATH, SERDE.serialize(PATH, OWNER), Optional.of(original.getVersion()),
                    EnumSet.of(CreateOption.Ephemeral)).get(5, TimeUnit.SECONDS);
            GetResult observed = backing.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow();
            CompletableFuture<Optional<GetResult>> metadataRead = new CompletableFuture<>();
            CompletableFuture<Void> readStarted = new CompletableFuture<>();
            doAnswer(invocation -> {
                readStarted.complete(null);
                return metadataRead;
            }).when(store).get(PATH);
            CompletableFuture<Void> update = lock.updateValue(OWNER);
            readStarted.get(5, TimeUnit.SECONDS);
            backing.delete(PATH, Optional.of(newer.getVersion())).get(5, TimeUnit.SECONDS);
            // Release must succeed when the node is already gone, even if the store wraps NotFoundException.
            doAnswer(invocation -> backing.delete(PATH, invocation.getArgument(1))
                    .exceptionallyCompose(error -> CompletableFuture.failedFuture(new ExecutionException(error))))
                    .when(store).delete(eq(PATH), any());
            // release() marks the handle as releasing right away, but deletes the lock only once
            // the update in flight completes
            CompletableFuture<Void> release = lock.release();

            metadataRead.complete(Optional.of(observed));
            assertThat(failure(update)).isInstanceOf(IllegalStateException.class);
            release.get(5, TimeUnit.SECONDS);
            // The delayed read must not have adopted the version it observed
            verify(store).delete(PATH, Optional.of(original.getVersion()));
            assertThat(failure(lock.updateValue("must-not-revive"))).isInstanceOf(IllegalStateException.class);
            assertThat(lock.getLockExpiredFuture()).isCompleted();
            assertThat(backing.get(PATH).get(5, TimeUnit.SECONDS)).isEmpty();
        }
    }

    @Test(timeOut = 10000)
    public void releaseDuringStaleLockReacquisitionRemovesReacquiredNode() throws Exception {
        try (MetadataStoreExtended backing = newStore()) {
            MetadataStoreExtended store = spy(backing);
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(store, SERDE, PATH,
                    mock(ScheduledExecutorService.class));
            lock.acquire(OWNER).get(5, TimeUnit.SECONDS);
            Stat acquired = backing.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow().getStat();
            Stat stale = backing.put(PATH, SERDE.serialize(PATH, OWNER), Optional.of(acquired.getVersion()),
                    EnumSet.of(CreateOption.Ephemeral)).get(5, TimeUnit.SECONDS);
            // LocalMemory has only one session. Report the node as created by an earlier session,
            // so that revalidation deletes it and acquires the lock again.
            Stat earlierSession = new Stat(PATH, stale.getVersion(), stale.getCreationTimestamp(),
                    stale.getModificationTimestamp(), true, false);
            doReturn(CompletableFuture.completedFuture(Optional.of(
                    new GetResult(SERDE.serialize(PATH, OWNER), earlierSession))))
                    .when(store).get(PATH);
            // Apply the deletion of the stale node, but hold back its outcome until release has started
            CompletableFuture<Void> staleDeleteApplied = new CompletableFuture<>();
            CompletableFuture<Void> staleDeleteOutcome = new CompletableFuture<>();
            doAnswer(invocation -> backing.delete(PATH, Optional.of(stale.getVersion()))
                    .thenRun(() -> staleDeleteApplied.complete(null))
                    .thenCompose(__ -> staleDeleteOutcome))
                    .when(store).delete(PATH, Optional.of(stale.getVersion()));

            lock.lockWasInvalidated();
            staleDeleteApplied.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> release = lock.release();
            staleDeleteOutcome.complete(null);

            release.get(5, TimeUnit.SECONDS);
            assertThat(lock.getLockExpiredFuture()).isCompleted();
            // The node that revalidation acquired again must not outlive the released handle
            assertThat(backing.get(PATH).get(5, TimeUnit.SECONDS)).isEmpty();
        }
    }

    @Test
    public void updateValueReportsClosedStore() throws Exception {
        var putFailure = new AtomicReference<CompletionException>();
        ResourceLockImpl<String> lock;
        try (var store = new FaultInjectionMetadataStore(newStore()) {
            @Override
            public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> version, Set<Option> opts) {
                return super.put(path, value, version, opts).exceptionally(error -> {
                    CompletionException failure = FutureUtil.wrapToCompletionException(error);
                    // Keep the write failure so the test can detect if the lock rewraps the same cause.
                    putFailure.set(failure);
                    throw failure;
                });
            }
        }) {
            lock = new ResourceLockImpl<>(store, SERDE, PATH, mock(ScheduledExecutorService.class));
            lock.acquire(OWNER).get(5, TimeUnit.SECONDS);
        }

        Throwable failure = lock.updateValue("updated-owner")
                .handle((ignored, error) -> error)
                .get(5, TimeUnit.SECONDS);
        assertThat(failure).isSameAs(putFailure.get())
                .hasCauseInstanceOf(AlreadyClosedException.class);
    }

    @Test
    public void releaseReportsVersionConflictAndPreservesNewerValue() throws Exception {
        try (MetadataStoreExtended store = newStore()) {
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(store, SERDE, PATH,
                    mock(ScheduledExecutorService.class));
            lock.acquire(OWNER).get(5, TimeUnit.SECONDS);
            byte[] newerValue = SERDE.serialize(PATH, "updated-owner");
            store.put(PATH, newerValue, Optional.empty(), EnumSet.of(CreateOption.Ephemeral))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> lock.release().get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(BadVersionException.class);
            assertThat(store.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow().getValue()).isEqualTo(newerValue);
            assertThat(lock.getLockExpiredFuture()).isNotDone();
        }
    }

    private static MetadataStoreExtended newStore() throws Exception {
        return MetadataStoreExtended.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
    }

    private static Throwable failure(CompletableFuture<Void> result) throws Exception {
        return result.handle((ignored, error) -> FutureUtil.unwrapCompletionException(error))
                .get(5, TimeUnit.SECONDS);
    }
}
