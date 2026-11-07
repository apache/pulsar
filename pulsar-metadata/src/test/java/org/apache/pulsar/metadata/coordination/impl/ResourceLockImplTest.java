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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.Test;

public class ResourceLockImplTest {

    /**
     * A revalidation whose read is still in flight when the lock is released must not
     * re-create the path behind the release.
     */
    @Test(timeOut = 20000)
    public void inFlightRevalidationDoesNotResurrectAReleasedLock() {
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        AtomicLong versions = new AtomicLong();
        when(store.put(anyString(), any(), any(), ArgumentMatchers.<EnumSet<CreateOption>>any()))
                .thenAnswer(invocation ->
                        CompletableFuture.completedFuture(new Stat(invocation.getArgument(0),
                                versions.incrementAndGet(), 0, 0, true, true)));
        when(store.delete(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Optional<GetResult>> pendingRead = new CompletableFuture<>();
        when(store.get(anyString())).thenReturn(pendingRead);

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        LockManagerImpl<String> lockManager = new LockManagerImpl<>(store, String.class, executor);

        ResourceLockImpl<String> lock =
                (ResourceLockImpl<String>) lockManager.acquireLock("/lock", "value").join();
        assertThat(versions.get()).isEqualTo(1);

        // The revalidation is in flight when the lock is released.
        lock.silentRevalidateOnce();
        CompletableFuture<Void> release = lock.release();

        // The in-flight read completes with the record gone: the revalidation must not
        // re-create it behind the release.
        pendingRead.complete(Optional.empty());
        release.join();

        verify(store, times(1)).put(anyString(), any(), any(),
                ArgumentMatchers.<EnumSet<CreateOption>>any());
    }

    /**
     * A create that is still in flight when the handle is superseded and retired must not
     * resurrect the path: its completion retracts the write it landed, version-compared
     * with the version the write returned.
     */
    @Test(timeOut = 20000)
    public void retiredHandleWithInFlightCreateDoesNotResurrectThePath() {
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        CompletableFuture<Stat> heldCreate = new CompletableFuture<>();
        Queue<CompletableFuture<Stat>> putResults = new ArrayDeque<>(List.of(
                CompletableFuture.completedFuture(new Stat("/lock", 1, 0, 0, true, true)),
                heldCreate,
                CompletableFuture.completedFuture(new Stat("/lock", 2, 0, 0, true, true))));
        when(store.put(anyString(), any(), any(), ArgumentMatchers.<EnumSet<CreateOption>>any()))
                .thenAnswer(invocation -> putResults.remove());
        when(store.get(anyString())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(store.delete(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        LockManagerImpl<String> lockManager = new LockManagerImpl<>(store, String.class, executor);

        ResourceLockImpl<String> retired =
                (ResourceLockImpl<String>) lockManager.acquireLock("/lock", "value").join();

        // The revalidation's read finds the record gone, and its re-create is held in
        // flight.
        CompletableFuture<Void> revalidation = retired.silentRevalidateOnce();
        verify(store, timeout(5000).times(2)).put(anyString(), any(), any(),
                ArgumentMatchers.<EnumSet<CreateOption>>any());

        // The handle is superseded by a new acquire, which retires it, and the replacement
        // is released before the in-flight create lands.
        ResourceLockImpl<String> replacement =
                (ResourceLockImpl<String>) lockManager.acquireLock("/lock", "value").join();
        assertThat(retired.getLockExpiredFuture()).isDone();
        replacement.release().join();

        // The create lands now: its completion retracts the record it wrote, deleting it
        // with the version the write returned, instead of marking the retired handle valid.
        heldCreate.complete(new Stat("/lock", 3, 0, 0, true, true));
        revalidation.join();
        verify(store).delete("/lock", Optional.of(3L));
    }

    /**
     * On a store whose creator identity is session-scoped (the default, like the zk
     * backend), a self-created record is already bound to the live session: the adoption
     * costs no re-write, and the lock stays valid for later revalidations.
     */
    @Test(timeOut = 20000)
    public void adoptionOnSessionScopedStoreDoesNotRewrite() {
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        when(store.isCreatorIdentitySessionScoped()).thenReturn(true);
        AtomicLong versions = new AtomicLong();
        when(store.put(anyString(), any(), any(), ArgumentMatchers.<EnumSet<CreateOption>>any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(new Stat(
                        invocation.getArgument(0), versions.incrementAndGet(), 0, 0, true, true)));
        AtomicReference<Optional<GetResult>> nextRead = new AtomicReference<>(Optional.empty());
        when(store.get(anyString()))
                .thenAnswer(__ -> CompletableFuture.completedFuture(nextRead.get()));

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        LockManagerImpl<String> lockManager = new LockManagerImpl<>(store, String.class, executor);

        ResourceLockImpl<String> lock =
                (ResourceLockImpl<String>) lockManager.acquireLock("/lock", "value").join();

        // The record is still there and still reads as created by this client on the live
        // session: the revalidation adopts it without a write.
        ArgumentCaptor<byte[]> writtenValue = ArgumentCaptor.forClass(byte[].class);
        verify(store).put(anyString(), writtenValue.capture(), any(),
                ArgumentMatchers.<EnumSet<CreateOption>>any());
        nextRead.set(Optional.of(new GetResult(writtenValue.getValue(),
                new Stat("/lock", 1, 0, 0, true, true))));
        lock.silentRevalidateOnce().join();
        verify(store, times(1)).put(anyString(), any(), any(),
                ArgumentMatchers.<EnumSet<CreateOption>>any());

        // The adoption left the lock valid: when the record is gone, the next revalidation
        // re-creates it.
        nextRead.set(Optional.empty());
        lock.silentRevalidateOnce().join();
        verify(store, times(2)).put(anyString(), any(), any(),
                ArgumentMatchers.<EnumSet<CreateOption>>any());
    }

    /**
     * On a store whose creator identity outlives the session (the oxia backend), a
     * self-created record may still be bound to an expired session: the adoption re-writes
     * it with the version the read returned, re-binding it to the live session.
     */
    @Test(timeOut = 20000)
    public void adoptionOnClientScopedStoreRewrites() {
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        when(store.isCreatorIdentitySessionScoped()).thenReturn(false);
        AtomicLong versions = new AtomicLong();
        when(store.put(anyString(), any(), any(), ArgumentMatchers.<EnumSet<CreateOption>>any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(new Stat(
                        invocation.getArgument(0), versions.incrementAndGet(), 0, 0, true, true)));
        AtomicReference<Optional<GetResult>> nextRead = new AtomicReference<>(Optional.empty());
        when(store.get(anyString()))
                .thenAnswer(__ -> CompletableFuture.completedFuture(nextRead.get()));

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        LockManagerImpl<String> lockManager = new LockManagerImpl<>(store, String.class, executor);

        ResourceLockImpl<String> lock =
                (ResourceLockImpl<String>) lockManager.acquireLock("/lock", "value").join();

        ArgumentCaptor<byte[]> writtenValue = ArgumentCaptor.forClass(byte[].class);
        verify(store).put(anyString(), writtenValue.capture(), any(),
                ArgumentMatchers.<EnumSet<CreateOption>>any());
        nextRead.set(Optional.of(new GetResult(writtenValue.getValue(),
                new Stat("/lock", 1, 0, 0, true, true))));

        // The revalidation re-writes the record with the version the read returned.
        lock.silentRevalidateOnce().join();
        verify(store, times(2)).put(anyString(), any(), any(),
                ArgumentMatchers.<EnumSet<CreateOption>>any());
        verify(store).put(anyString(), any(), eq(Optional.of(1L)),
                ArgumentMatchers.<EnumSet<CreateOption>>any());
    }
}
