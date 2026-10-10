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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
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
}
