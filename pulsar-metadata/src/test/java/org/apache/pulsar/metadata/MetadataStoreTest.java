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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import lombok.Cleanup;

import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MetadataStoreTest {

    private TestZKServer zks;

    @BeforeClass
    void setup() throws Exception {
        zks = new TestZKServer();
    }

    @AfterClass
    void teardown() throws Exception {
        zks.close();
    }

    @DataProvider(name = "impl")
    public Object[][] implementations() {
        return new Object[][] {
                { "ZooKeeper", zks.getConnectionString() },
        };
    }

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
            assertEquals(e.getCause().getClass(), NotFoundException.class);
        }

        try {
            store.delete("/non-existing-key", Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), NotFoundException.class);
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
            assertEquals(e.getCause().getClass(), BadVersionException.class);
        }

        try {
            store.put(key1, "value-1".getBytes(), Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), BadVersionException.class);
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
            assertEquals(e.getCause().getClass(), BadVersionException.class);
        }

        try {
            store.put(key1, "value-2".getBytes(), Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), BadVersionException.class);
        }

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
            assertEquals(e.getCause().getClass(), MetadataStoreException.class);
        }

        for (int i = 0; i < N; i++) {
            try {
                store.delete(key + "/c-" + i, Optional.of(1L)).join();
                fail("The key has children");
            } catch (CompletionException e) {
                assertEquals(e.getCause().getClass(), BadVersionException.class);
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
            assertEquals(e.getCause().getClass(), MetadataStoreException.class);
        }

        try {
            store.getChildren("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.class);
        }

        try {
            store.get("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.class);
        }

        try {
            store.exists("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.class);
        }

        try {
            store.put("", new byte[0], Optional.empty()).join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.class);
        }
    }

    private static String newKey() {
        return "/key-" + System.nanoTime();
    }
}
