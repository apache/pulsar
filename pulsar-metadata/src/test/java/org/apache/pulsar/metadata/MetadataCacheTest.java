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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;

import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.cache.MetadataCache;
import org.testng.annotations.Test;

public class MetadataCacheTest extends BaseMetadataStoreTest {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class MyClass {
        String a;
        int b;
    }

    @Test(dataProvider = "impl")
    public void emptyCacheTest(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        assertEquals(objCache.getIfCached("/non-existing-key"), Optional.empty());
        assertEquals(objCache.getIfCached("/non-existing-key/child"), Optional.empty());

        assertEquals(objCache.get("/non-existing-key").join(), Optional.empty());
        assertEquals(objCache.get("/non-existing-key/child").join(), Optional.empty());

        try {
            objCache.delete("/non-existing-key").join();
            fail("should have failed");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), NotFoundException.class);
        }

        try {
            objCache.delete("/non-existing-key/child").join();
            fail("should have failed");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), NotFoundException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void insertionDeletionWitGenericType(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        MetadataCache<Map<String, String>> objCache = store
                .getMetadataCache(new TypeReference<Map<String, String>>() {
                });

        String key1 = newKey();

        assertEquals(objCache.getIfCached(key1), Optional.empty());

        Map<String, String> v = new TreeMap<>();
        v.put("a", "1");
        v.put("b", "2");
        objCache.create(key1, v).join();

        assertEquals(objCache.getIfCached(key1), Optional.of(v));
        assertEquals(objCache.get(key1).join(), Optional.of(v));

        objCache.delete(key1).join();

        assertEquals(objCache.getIfCached(key1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void insertionDeletion(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());
        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        assertEquals(objCache.getIfCached(key1), Optional.empty());

        MyClass value1 = new MyClass("a", 1);
        objCache.create(key1, value1).join();

        MyClass value2 = new MyClass("a", 2);

        try {
            objCache.create(key1, value2).join();
            fail("should have failed to create");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), AlreadyExistsException.class);
        }

        assertEquals(objCache.getIfCached(key1), Optional.of(value1));
        assertEquals(objCache.get(key1).join(), Optional.of(value1));

        objCache.delete(key1).join();

        assertEquals(objCache.getIfCached(key1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void insertionOutsideCache(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());
        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        MyClass value1 = new MyClass("a", 1);
        store.put(key1, ObjectMapperFactory.getThreadLocal().writeValueAsBytes(value1), Optional.of(-1L)).join();

        assertEquals(objCache.getIfCached(key1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.of(value1));
    }

    @Test(dataProvider = "impl")
    public void insertionOutsideCacheWithGenericType(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());
        MetadataCache<Map<String, String>> objCache = store
                .getMetadataCache(new TypeReference<Map<String, String>>() {
                });

        String key1 = newKey();

        Map<String, String> v = new TreeMap<>();
        v.put("a", "1");
        v.put("b", "2");
        store.put(key1, ObjectMapperFactory.getThreadLocal().writeValueAsBytes(v), Optional.of(-1L)).join();

        assertEquals(objCache.getIfCached(key1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.of(v));
    }

    @Test(dataProvider = "impl")
    public void invalidJsonContent(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        store.put(key1, "-------".getBytes(), Optional.of(-1L)).join();

        try {
            objCache.get(key1).join();
            fail("should have failed to deserialize");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), ContentDeserializationException.class);
        }
        assertEquals(objCache.getIfCached(key1), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void readModifyUpdate(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        MyClass value1 = new MyClass("a", 1);
        objCache.create(key1, value1).join();

        objCache.readModifyUpdate(key1, v -> {
            return new MyClass(v.a, v.b + 1);
        }).join();

        Optional<MyClass> newValue1 = objCache.get(key1).join();
        assertTrue(newValue1.isPresent());
        assertEquals(newValue1.get().a, "a");
        assertEquals(newValue1.get().b, 2);

        // Should fail if the key does not exist
        try {
            objCache.readModifyUpdate(newKey(), v -> {
                return new MyClass(v.a, v.b + 1);
            }).join();
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), NotFoundException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void readModifyUpdateOrCreate(String provider, String url) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        objCache.readModifyUpdateOrCreate(key1, optValue -> {
            if (optValue.isPresent()) {
                return new MyClass(optValue.get().a, optValue.get().b + 1);
            } else {
                return new MyClass("a", 1);
            }
        }).join();

        Optional<MyClass> newValue1 = objCache.get(key1).join();
        assertTrue(newValue1.isPresent());
        assertEquals(newValue1.get().a, "a");
        assertEquals(newValue1.get().b, 1);

        objCache.readModifyUpdateOrCreate(key1, optValue -> {
            assertTrue(optValue.isPresent());
            return new MyClass(optValue.get().a, optValue.get().b + 1);
        }).join();

        newValue1 = objCache.get(key1).join();
        assertTrue(newValue1.isPresent());
        assertEquals(newValue1.get().a, "a");
        assertEquals(newValue1.get().b, 2);
    }

}
