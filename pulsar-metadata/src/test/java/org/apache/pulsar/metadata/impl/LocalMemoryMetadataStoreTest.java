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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import lombok.Cleanup;
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

    @Test
    public void testMetadataSyncWithSimpleRegexExclusions() throws Exception {
        // Create synchronizer with exclusions
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        sync.addRegexExclusions("/admin/.*");
        sync.addRegexExclusions("/temp/session.*");


        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        // Test 1: Path should be excluded (/admin/)
        String excludedPath1 = "/admin/schemas";
        store1.put(excludedPath1, value, Optional.empty()).join();

        // Wait a bit and verify that the event was not notified
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath1),
                "Event for /admin/schemas should be excluded");

        // Test 2: Another excluded path (/temp/session123)
        String excludedPath2 = "/temp/session123";
        store1.put(excludedPath2, value, Optional.empty()).join();

        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath2),
                "Event for /temp/session123 should be excluded");

        // Test 3: Path that should not be excluded
        String normalPath = "/data/test";
        store1.put(normalPath, value, Optional.empty()).join();

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(normalPath) != null);
        assertNotNull(sync.notifiedEvents.get(normalPath),
                "Event for /data/test path should be notified");
    }

    @Test
    public void testMetadataSyncWithNegativeLookaheadRegex() throws Exception {
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        // Exclude all path EXCEPT those starting with /important
        sync.addRegexExclusions("^(?!/important/).*");

        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        // Test 1: Path starting with /important/ - should be notified
        String importantPath = "/important/config";
        store1.put(importantPath, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(importantPath) != null);
        assertNotNull(sync.notifiedEvents.get(importantPath),
                "Event for /important/config path should be notified (negative lookahead doesn't match)");

        // Test 2: Other path - should be excluded
        String excludedPath1 = "/temp/data";
        store1.put(excludedPath1, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath1),
                "Event for /temp/data should be excluded (matches negative lookahead)");

        // Test 3: Another non-important path - should be excluded
        String excludedPath2 = "/admin/schemas";
        store1.put(excludedPath2, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath2),
                "Event for /admin/schemas should be excluded (matches negative lookahead)");
    }

    @Test
    public void testMetadataSyncWithComplexCharacterClassRegex() throws Exception {
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        // Exclude /namespace/{lowercase-only}/temp*
        sync.addRegexExclusions("/namespace/[a-z]+/temp.*");
        // Exclude admin test resources for schemas or topics
        sync.addRegexExclusions("/admin/(schemas|topics)/test.*");

        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        // Test 1: Matches character class pattern
        String excludedPath1 = "/namespace/public/temp123";
        store1.put(excludedPath1, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath1),
                "Event for /namespace/public/temp123 should be excluded");

        // Test 2: Doesn't match character class (has numbers)
        String normalPath1 = "/namespace/public123/temp";
        store1.put(normalPath1, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(normalPath1) != null);
        assertNotNull(sync.notifiedEvents.get(normalPath1),
                "Event for /namespace/public123/temp path should be notified");

        // Test 3: Matches alternation (schemas)
        String excludedPath2 = "/admin/schemas/test-schema";
        store1.put(excludedPath2, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath2),
                "Event for /admin/schemas/test-schema should be excluded");

        // Test 4: Matches alternation (topics)
        String excludedPath3 = "/admin/topics/test-topic";
        store1.put(excludedPath3, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath3),
                "Event for /admin/topics/test-topic should be excluded");

        // Test 5: Doesn't match alternation
        String normalPath2 = "/admin/functions/test-function";
        store1.put(normalPath2, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(normalPath2) != null);
        assertNotNull(sync.notifiedEvents.get(normalPath2),
                "Event for /admin/functions/test-function path should be notified");
    }

    @Test
    public void testMetadataSyncWithMultipleNegativeLookaheadRegex() throws Exception {
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        // Exclude all path EXCEPT those starting with /production/ OR /critical/
        // Using negative lookahead with alternation
        sync.addRegexExclusions("^(?!/(production|critical)/).*");

        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        // Test 1: Path starting with /production/ - should be notified
        String productionPath = "/production/config";
        store1.put(productionPath, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(productionPath) != null);
        assertNotNull(sync.notifiedEvents.get(productionPath),
                "Event for /production/config path should be notified");

        // Test 2: Path starting with /critical/ - should be notified
        String criticalPath = "/critical/data";
        store1.put(criticalPath, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(criticalPath) != null);
        assertNotNull(sync.notifiedEvents.get(criticalPath),
                "Event for /critical/data path should be notified");

        // Test 3: Other path - should be excluded
        String excludedPath = "excludedPath";
        store1.put(excludedPath, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath),
                "Event for excludedPath should be excluded");
    }

    @Test
    public void testMetadataSyncWithAnchoredRegex() throws Exception {
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        sync.addRegexExclusions("^/admin/.*"); // Anchored at start
        sync.addRegexExclusions(".*/tmp$"); //Anchored at end

        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        // Test 1: Matches start anchor
        String excludedPath1 = "/admin/schemas";
        store1.put(excludedPath1, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath1),
                "Event for /admin/schemas should be excluded");

        // Test 2: Matches end anchor
        String excludedPath2 = "/data/tmp";
        store1.put(excludedPath2, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath2),
                "Event for /data/tmp should be excluded");

        // Test 3: Doesn't match end anchor
        String normalPath = "/data/tmp/file";
        store1.put(normalPath, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(normalPath) != null);
        assertNotNull(sync.notifiedEvents.get(normalPath),
                "Event for /data/tmp/file path should be notified");

    }

    @Test
    public void testMetadataSyncWithCombinedComplexRegex() throws Exception {
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        // Complex pattern: Exclude /cache/* OR /temp/* but NOT if the path ends with important
        sync.addRegexExclusions("^/(cache|temp)/(?!.*important$).*");


        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        // Test 1: /cache path without important - should be excluded
        String excludedPath1 = "/cache/data";
        store1.put(excludedPath1, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath1),
                "Event for /cache/data should be excluded");

        // Test 2: /temp path without important - should be excluded
        String excludedPath2 = "/temp/session";
        store1.put(excludedPath2, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath2),
                "Event for /temp/session should be excluded");

        // Test 3: /cache path ending with important - should be notified
        String normalPath1 = "/cache/data/important";
        store1.put(normalPath1, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(normalPath1) != null);
        assertNotNull(sync.notifiedEvents.get(normalPath1),
                "Event for /cache/data/important path should be notified");

        // Test 4: Different path - should be notified
        String normalPath2 = "/data/test";
        store1.put(normalPath2, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(normalPath2) != null);
        assertNotNull(sync.notifiedEvents.get(normalPath2),
                "Event for /data/test path should be notified");
    }

    @Test
    public void testMetadataSyncWithDeleteAndComplexRegex() throws Exception {
        TestMetadataEventSynchronizer sync = new TestMetadataEventSynchronizer();
        // Exclude temporary or cache paths
        sync.addRegexExclusions("^/(cache|temp)/.*");


        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().synchronizer(sync).build());

        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        // Test 1: Put and delete on excluded path
        String excludedPath = "/temp/session";
        store1.put(excludedPath, value, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath),
                "Put event for /temp/session should be excluded");
        store1.delete(excludedPath, Optional.empty()).join();
        Thread.sleep(100);
        assertNull(sync.notifiedEvents.get(excludedPath),
                "Delete event for /temp/session should be excluded");

        // Test 2: Put and delete on normal path
        String normalPath = "/data/test";
        store1.put(normalPath, value, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(normalPath) != null);
        assertNotNull(sync.notifiedEvents.get(normalPath),
                "Put event for /data/test should be notified");
        assertEquals(sync.notifiedEvents.get(normalPath).getType(), NotificationType.Modified);
        store1.delete(normalPath, Optional.empty()).join();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                sync.notifiedEvents.get(normalPath) != null);
        assertNotNull(sync.notifiedEvents.get(normalPath),
                "Delete event for /data/test should be notified");
        assertEquals(sync.notifiedEvents.get(normalPath).getType(), NotificationType.Deleted);

    }

    static class TestMetadataEventSynchronizer implements MetadataEventSynchronizer {
        public Map<String, MetadataEvent> notifiedEvents = new ConcurrentHashMap<>();
        public String clusterName = "test";
        public volatile Function<MetadataEvent, CompletableFuture<Void>> listener;
        private final List<Pattern> exclusionPatterns = new CopyOnWriteArrayList<>();

        public void addRegexExclusions(String regexPattern) {
            Pattern pattern = Pattern.compile(regexPattern);
            exclusionPatterns.add(pattern);
        }

        private boolean isExcluded(String path) {
            if (path == null) {
                return false;
            }
            // Check regex pattern exclusions
            if (!exclusionPatterns.isEmpty()){
                for (Pattern pattern : exclusionPatterns) {
                    if(pattern.matcher(path).matches()) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public CompletableFuture<Void> notify(MetadataEvent event) {
            // Check if the event path is excluded
            if (isExcluded(event.getPath())){
                return CompletableFuture.completedFuture(null);
            }
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
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }

    }
}
