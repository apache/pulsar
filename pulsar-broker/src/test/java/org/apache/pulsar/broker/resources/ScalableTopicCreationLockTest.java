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
package org.apache.pulsar.broker.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.pulsar.broker.resources.ScalableTopicResources.CreationLock;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.DualMetadataStore;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * The creation lock of a scalable topic ({@link ScalableTopicResources#runWithCreationLock}), which a creation of the
 * topic and a migration of a regular topic to it take, so they exclude each other on every broker.
 */
public class ScalableTopicCreationLockTest {

    private static final TopicName TOPIC = TopicName.get("topic://tenant/ns/orders");

    /** Take the lock, complete {@code held} with it, and hold it until {@code release} completes. */
    private static CompletableFuture<Void> holdLock(ScalableTopicResources resources,
                                                    CompletableFuture<CreationLock> held,
                                                    CompletableFuture<Void> release) {
        return resources.runWithCreationLock(TOPIC, lock -> {
            held.complete(lock);
            return release;
        });
    }

    private static void assertLockBusy(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(MetadataStoreException.LockBusyException.class);
    }

    @Test
    public void testAcquisitionsOfATopicExcludeEachOther() throws Exception {
        @Cleanup
        MetadataStoreExtended store = new LocalMemoryMetadataStore("memory:exclusion",
                MetadataStoreConfig.builder().build());
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);
        CompletableFuture<CreationLock> held = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        CompletableFuture<Void> holder = holdLock(resources, held, release);
        held.get(10, TimeUnit.SECONDS);

        // Refused even on the same broker, with the same metadata session.
        AtomicBoolean ran = new AtomicBoolean();
        assertLockBusy(resources.runWithCreationLock(TOPIC, lock -> {
            ran.set(true);
            return CompletableFuture.completedFuture(null);
        }));
        assertThat(ran).isFalse();
        // The lock of another topic is another lock.
        resources.runWithCreationLock(TopicName.get("topic://tenant/ns/payments"), CreationLock::checkHeld)
                .get(10, TimeUnit.SECONDS);

        release.complete(null);
        holder.get(10, TimeUnit.SECONDS);
        resources.runWithCreationLock(TOPIC, CreationLock::checkHeld).get(10, TimeUnit.SECONDS);
    }

    /**
     * When the store retried the create of the lock's record after losing its response, and found the record there,
     * the acquisition holds the lock: the record is its own.
     */
    @Test
    public void testAcquisitionWhoseCreateResponseWasLost() throws Exception {
        AtomicBoolean responseLost = new AtomicBoolean();
        @Cleanup
        MetadataStoreExtended store = new LocalMemoryMetadataStore("memory:lost-response",
                MetadataStoreConfig.builder().build()) {
            @Override
            public CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                                    Set<Option> opts) {
                CompletableFuture<Stat> put = super.storePut(path, data, optExpectedVersion, opts);
                if (path.startsWith("/topic-creation-locks/") && responseLost.compareAndSet(false, true)) {
                    // The record is created, but the response is lost, and the create retried finds the record.
                    return put.thenCompose(__ -> CompletableFuture.failedFuture(
                            new MetadataStoreException.BadVersionException("The record exists")));
                }
                return put;
            }
        };
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);

        assertThat(resources.runWithCreationLock(TOPIC, lock -> lock.checkHeld().thenApply(__ -> "created"))
                .get(10, TimeUnit.SECONDS)).isEqualTo("created");
        assertThat(responseLost).isTrue();
        // Released: the next acquisition takes the lock.
        resources.runWithCreationLock(TOPIC, CreationLock::checkHeld).get(10, TimeUnit.SECONDS);
    }

    /** An acquisition that fails after creating its record, as when it can't list the records, deletes it. */
    @Test
    public void testAcquisitionThatFailsToListTheRecords() throws Exception {
        AtomicBoolean listingFailed = new AtomicBoolean();
        @Cleanup
        MetadataStoreExtended store = new LocalMemoryMetadataStore("memory:failed-listing",
                MetadataStoreConfig.builder().build()) {
            @Override
            public CompletableFuture<List<String>> getChildrenFromStore(String path, Set<Option> opts) {
                if (path.startsWith("/topic-creation-locks/") && listingFailed.compareAndSet(false, true)) {
                    return CompletableFuture.failedFuture(new MetadataStoreException("Injected listing failure"));
                }
                return super.getChildrenFromStore(path, opts);
            }
        };
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);

        AtomicBoolean ran = new AtomicBoolean();
        assertThatThrownBy(() -> resources.runWithCreationLock(TOPIC, lock -> {
                    ran.set(true);
                    return CompletableFuture.completedFuture(null);
                }).get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("Injected listing failure");
        assertThat(ran).isFalse();
        // Its record was deleted: the next acquisition takes the lock.
        resources.runWithCreationLock(TOPIC, CreationLock::checkHeld).get(10, TimeUnit.SECONDS);
    }

    /**
     * A broker whose metadata session expired lost the lock, which another broker can take. The first broker no
     * longer holds it, and releasing it when its action completes leaves the lock of the other broker in place.
     */
    @Test
    public void testLockLostWithTheMetadataSession() throws Exception {
        @Cleanup
        TestZKServer zks = new TestZKServer();
        @Cleanup
        MetadataStoreExtended storeA = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().build());
        @Cleanup
        MetadataStoreExtended storeB = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().build());
        ScalableTopicResources resourcesA = new ScalableTopicResources(storeA, 30);
        ScalableTopicResources resourcesB = new ScalableTopicResources(storeB, 30);

        CompletableFuture<CreationLock> heldByA = new CompletableFuture<>();
        CompletableFuture<Void> releaseA = new CompletableFuture<>();
        CompletableFuture<Void> holderA = holdLock(resourcesA, heldByA, releaseA);
        CreationLock lockA = heldByA.get(10, TimeUnit.SECONDS);

        BlockingQueue<SessionEvent> sessionEventsA = new LinkedBlockingQueue<>();
        storeA.registerSessionListener(sessionEventsA::add);
        zks.expireSession(((ZKMetadataStore) ((DualMetadataStore) storeA).getSourceStore()).getZkClient()
                .getSessionId());
        Awaitility.await().until(() -> sessionEventsA.contains(SessionEvent.SessionReestablished));

        CompletableFuture<CreationLock> heldByB = new CompletableFuture<>();
        CompletableFuture<Void> releaseB = new CompletableFuture<>();
        CompletableFuture<Void> holderB = holdLock(resourcesB, heldByB, releaseB);
        CreationLock lockB = heldByB.get(10, TimeUnit.SECONDS);

        assertLockBusy(lockA.checkHeld());
        releaseA.complete(null);
        holderA.get(10, TimeUnit.SECONDS);

        lockB.checkHeld().get(10, TimeUnit.SECONDS);
        assertLockBusy(resourcesA.runWithCreationLock(TOPIC, CreationLock::checkHeld));
        releaseB.complete(null);
        holderB.get(10, TimeUnit.SECONDS);
    }
}
