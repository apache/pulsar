/**
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.testng.annotations.Test;

public class MetadataStoreTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void emptyStoreTest(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        assertFalse(store.exists("/non-existing-key").join());
        assertFalse(store.exists("/non-existing-key/child").join());
        assertFalse(store.get("/non-existing-key").join().isPresent());
        assertFalse(store.get("/non-existing-key/child").join().isPresent());

        assertEquals(store.getChildren("/non-existing-key").join(), Collections.emptyList());
        assertEquals(store.getChildren("/non-existing-key/child").join(), Collections.emptyList());

        try {
            store.delete("/non-existing-key", Optional.empty()).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, NotFoundException.class);
        }

        try {
            store.delete("/non-existing-key", Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, NotFoundException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void insertionTestWithExpectedVersion(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        String key1 = newKey();

        try {
            store.put(key1, "value-1".getBytes(), Optional.of(0L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        try {
            store.put(key1, "value-1".getBytes(), Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        store.put(key1, "value-1".getBytes(), Optional.of(-1L)).join();

        assertTrue(store.exists(key1).join());
        Optional<GetResult> optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-1".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), 0);

        try {
            store.put(key1, "value-2".getBytes(), Optional.of(-1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        try {
            store.put(key1, "value-2".getBytes(), Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        assertTrue(store.exists(key1).join());
        optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-1".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), 0);

        store.put(key1, "value-2".getBytes(), Optional.of(0L)).join();

        assertTrue(store.exists(key1).join());
        optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-2".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), 1);
    }

    @Test(dataProvider = "impl")
    public void getChildrenTest(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        String key = newKey();
        int N = 10;
        List<String> expectedChildren = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            store.put(key + "/c-" + i, new byte[0], Optional.empty()).join();

            expectedChildren.add("c-" + i);
        }

        assertEquals(store.getChildren(key).join(), expectedChildren);

        // Nested children
        for (int i = 0; i < N; i++) {
            store.put(key + "/c-0/cc-" + i, new byte[0], Optional.empty()).join();
        }

        assertEquals(store.getChildren(key).join(), expectedChildren);
    }

    @Test(dataProvider = "impl")
    public void deletionTest(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        String key = newKey();
        int N = 10;
        List<String> expectedChildren = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            store.put(key + "/c-" + i, new byte[0], Optional.empty()).join();

            expectedChildren.add("c-" + i);
        }

        try {
            store.delete(key, Optional.empty()).join();
            fail("The key has children");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        for (int i = 0; i < N; i++) {
            try {
                store.delete(key + "/c-" + i, Optional.of(1L)).join();
                fail("The key has children");
            } catch (CompletionException e) {
                assertException(e, BadVersionException.class);
            }

            store.delete(key + "/c-" + i, Optional.empty()).join();
        }
    }

    @Test(dataProvider = "impl")
    public void emptyKeyTest(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        try {
            store.delete("", Optional.empty()).join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        try {
            store.getChildren("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        try {
            store.get("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        try {
            store.exists("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        try {
            store.put("", new byte[0], Optional.empty()).join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void notificationListeners(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        BlockingQueue<Notification> notifications = new LinkedBlockingDeque<>();
        store.registerListener(n -> {
            if (n.getType() != NotificationType.Invalidate) {
                notifications.add(n);
            }
        });

        String key1 = newKey();

        assertFalse(store.get(key1).join().isPresent());

        // Trigger created notification
        Stat stat = store.put(key1, "value-1".getBytes(), Optional.empty()).join();
        assertTrue(store.get(key1).join().isPresent());
        assertEquals(store.getChildren(key1).join(), Collections.emptyList());
        assertEquals(stat.getVersion(), 0);

        Notification n;
        if (provider.equals("Memory")) {
            n = notifications.poll(3, TimeUnit.SECONDS);
            assertNotNull(n);
            assertEquals(n.getType(), NotificationType.Created);
            assertEquals(n.getPath(), key1);
        } else {
            long zkSessionId = ((ZKMetadataStore) store).getZkSessionId();
            Collection<String> wchc = zks.wchc(zkSessionId);
            assertTrue(wchc.contains(key1));
        }

        // Trigger modified notification
        stat = store.put(key1, "value-2".getBytes(), Optional.empty()).join();
        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.Modified);
        assertEquals(n.getPath(), key1);
        assertEquals(stat.getVersion(), 1);

        // Trigger modified notification on the parent
        String key1Child = key1 + "/xx";

        assertFalse(store.get(key1Child).join().isPresent());

        store.put(key1Child, "value-2".getBytes(), Optional.empty()).join();
        if (provider.equals("Memory")) {
            n = notifications.poll(3, TimeUnit.SECONDS);
            assertNotNull(n);
            assertEquals(n.getType(), NotificationType.Created);
            assertEquals(n.getPath(), key1Child);
        }

        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.ChildrenChanged);
        assertEquals(n.getPath(), key1);

        assertTrue(store.exists(key1Child).join());
        assertEquals(store.getChildren(key1).join(), Collections.singletonList("xx"));

        store.delete(key1Child, Optional.empty()).join();
        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.Deleted);
        assertEquals(n.getPath(), key1Child);

        // Parent should be notified of the deletion
        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.ChildrenChanged);
        assertEquals(n.getPath(), key1);

        if (provider.equals("ZooKeeper")) {
            long zkSessionId = ((ZKMetadataStore) store).getZkSessionId();
            Collection<String> wchc = zks.wchc(zkSessionId);
            assertTrue(wchc.isEmpty());
        }
    }
}
