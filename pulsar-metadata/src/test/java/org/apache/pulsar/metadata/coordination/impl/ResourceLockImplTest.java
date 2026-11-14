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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
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
        try (MetadataStoreExtended store = newStore()) {
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

    @DataProvider
    public Object[][] releaseStates() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "releaseStates", timeOut = 10000)
    public void delayedSameSessionReadCannotReviveReleasingOrReleasedHandle(boolean releaseCompleted) throws Exception {
        try (MetadataStoreExtended backing = newStore()) {
            MetadataStoreExtended store = spy(backing);
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(store, SERDE, PATH,
                    mock(ScheduledExecutorService.class));
            lock.acquire(OWNER).get(5, TimeUnit.SECONDS);
            Stat original = backing.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow().getStat();
            // Make the public update encounter a version conflict and re-read the same session's
            // value. Delay that read's response until release has begun or fully completed.
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
            CompletableFuture<Void> metadataDelete = new CompletableFuture<>();
            doReturn(metadataDelete).when(store).delete(PATH, Optional.of(original.getVersion()));
            CompletableFuture<Void> release = lock.release();
            if (releaseCompleted) {
                metadataDelete.completeExceptionally(new NotFoundException(PATH));
                release.get(5, TimeUnit.SECONDS);
            }

            metadataRead.complete(Optional.of(observed));
            assertThat(failure(update)).isInstanceOf(IllegalStateException.class);
            assertThat(failure(lock.updateValue("must-not-revive"))).isInstanceOf(IllegalStateException.class);
            if (!releaseCompleted) {
                metadataDelete.completeExceptionally(new NotFoundException(PATH));
                release.get(5, TimeUnit.SECONDS);
            }
            assertThat(lock.getLockExpiredFuture()).isCompleted();
            assertThat(backing.get(PATH).get(5, TimeUnit.SECONDS)).isEmpty();
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
