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
package org.apache.pulsar.metadata.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

import lombok.Cleanup;

public class LocalMemoryMetadataStoreTest {

    HashSet<CreateOption> EMPTY_SET = new HashSet<>();
    @Test
    public void testNotifyEvent() throws Exception {
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        String path = "/test";
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        store1.put(path, value, Optional.empty()).join();

        assertTrue(store1.exists(path).join());
        MetadataEvent event = sync.notifiedEvents.get(path);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> event != null);
        assertNotNull(event);
        assertEquals(event.getPath(), path);
        assertEquals(event.getValue(), value);
        assertEquals(event.getOptions(), EMPTY_SET);
        assertEquals(event.getType(), NotificationType.Modified);
        assertEquals(event.getSourceCluster(), sync.clusterName);
        assertNull(event.getExpectedVersion());

        // (2) with expected version
        long exptectedVersion = 0L;
        for (; exptectedVersion < 4; exptectedVersion++) {
            sync.notifiedEvents.remove(path);
            store1.put(path, value, Optional.of(exptectedVersion)).join();
            MetadataEvent event2 = sync.notifiedEvents.get(path);
            Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> event2 != null);
            assertNotNull(event2);
            assertEquals(event2.getPath(), path);
            assertEquals((long) event2.getExpectedVersion(), exptectedVersion);
            assertEquals(event2.getType(), NotificationType.Modified);
        }

        // (3) delete node
        sync.notifiedEvents.remove(path);
        store1.delete(path, Optional.of(exptectedVersion)).join();
        MetadataEvent event2 = sync.notifiedEvents.get(path);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> event2 != null);
        assertNotNull(event2);
        assertEquals(event2.getPath(), path);
        assertEquals((long) event2.getExpectedVersion(), exptectedVersion);
        assertEquals(event2.getType(), NotificationType.Deleted);
        assertEquals(event2.getSourceCluster(), sync.clusterName);
        assertEquals(event2.getOptions(), EMPTY_SET);
    }

    @Test
    public void testIsIgnoreEvent() throws Exception {

        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        @Cleanup
        AbstractMetadataStore store1 = (AbstractMetadataStore) MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        String path = "/test";
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);
        store1.put(path, value1, Optional.empty()).join();

        long time1 = Instant.now().toEpochMilli();
        long time2 = time1 -5;
        Stat stats = new Stat(path, 0, time2, time2, false, false);
        GetResult eixistingData = new GetResult(value1, stats);
        // (1) ignore due to Ephemeral node
        MetadataEvent event = new MetadataEvent(path, value1, Sets.newHashSet(CreateOption.Ephemeral), 0L,
                time1, sync.getClusterName(), NotificationType.Modified);
        assertTrue(store1.shouldIgnoreEvent(event, eixistingData));
        // (2) ignore due to invalid expected version
        event = new MetadataEvent(path, value1, EMPTY_SET, 10L/*invalid-version*/,
                time1, sync.getClusterName(), NotificationType.Modified);
        assertTrue(store1.shouldIgnoreEvent(event, eixistingData));
        // (3) accept with valid conditions
        event = new MetadataEvent(path, value1, EMPTY_SET, 0L,
                time1, sync.getClusterName(), NotificationType.Modified);
        assertFalse(store1.shouldIgnoreEvent(event, eixistingData));
        // (4) Ignore due to invalid cluster name
        event = new MetadataEvent(path, value1, EMPTY_SET, 0L,
                time1, null, NotificationType.Modified);
        assertTrue(store1.shouldIgnoreEvent(event, eixistingData));
        // (5) consider due to same timestamp and correct expected version on the same cluster
        event = new MetadataEvent(path, value1, EMPTY_SET, 0L,
                time2, sync.getClusterName(), NotificationType.Modified);
        assertFalse(store1.shouldIgnoreEvent(event, eixistingData));
        // (6) Ignore due to same timestamp but different expected version on the same cluster
        event = new MetadataEvent(path, value1, EMPTY_SET, 10L,
                time2, sync.getClusterName(), NotificationType.Modified);
        assertTrue(store1.shouldIgnoreEvent(event, eixistingData));
        // (7) consider due to same timestamp but expected version=-1 on the same cluster
        event = new MetadataEvent(path, value1, EMPTY_SET, null,
                time2, sync.getClusterName(), NotificationType.Modified);
        assertFalse(store1.shouldIgnoreEvent(event, eixistingData));
        // (8) Ignore due to less timestamp on the same cluster
        event = new MetadataEvent(path, value1, EMPTY_SET, 0L,
                time2-5, sync.getClusterName(), NotificationType.Modified);
        assertTrue(store1.shouldIgnoreEvent(event, eixistingData));
        // (9) consider "uest" > "test" and same timestamp
        event = new MetadataEvent(path, value1, EMPTY_SET, 0L,
                time2, "uest", NotificationType.Modified);
        assertFalse(store1.shouldIgnoreEvent(event, eixistingData));
        // (10) ignore "uest" > "test" and less timestamp
        event = new MetadataEvent(path, value1, EMPTY_SET, 0L,
                time2-5, "uest", NotificationType.Modified);
        assertTrue(store1.shouldIgnoreEvent(event, eixistingData));
        // (11) ignore "rest" < "test" and same timestamp
        event = new MetadataEvent(path, value1, EMPTY_SET, 0L,
                time2, "rest", NotificationType.Modified);
        assertTrue(store1.shouldIgnoreEvent(event, eixistingData));
    }

    @Test
    public void testSyncListener() throws Exception {
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        String path = "/test";
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);
        store1.put(path, value1, Optional.empty()).join();

        assertTrue(store1.exists(path).join());

        Stat stats = store1.get(path).get().get().getStat();
        MetadataEvent event = new MetadataEvent(path, value2, EMPTY_SET, stats.getVersion(),
                stats.getModificationTimestamp() + 1, sync.clusterName, NotificationType.Modified);
        sync.listener.apply(event).get();
        assertEquals(store1.get(path).get().get().getValue(), value2);
    }

    static class TestMetadataEventSynchronizer implements MetadataEventSynchronizer {
        public Map<String, MetadataEvent> notifiedEvents = new ConcurrentHashMap<>();
        public String clusterName = "test";
        public volatile Function<MetadataEvent, CompletableFuture<Void>> listener;

        @Override
        public CompletableFuture<Void> notify(MetadataEvent event) {
            notifiedEvents.put(event.getPath(), event);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void registerSyncListener(Function<MetadataEvent, CompletableFuture<Void>> fun) {
            this.listener = fun;
        }

        @Override
        public String getClusterName() {
            return clusterName;
        }

        @Override
        public void close() {
            // No-op
        }

    }
}
