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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

/**
 * Coverage for the {@link MetadataCache} overloads that accept an
 * {@code indexExtractor}: every cache write must register the indexes derived
 * from the value with the underlying store, so a subsequent
 * {@link org.apache.pulsar.metadata.api.MetadataStore#findByIndex} returns the
 * record. Updates must refresh the indexes so that the post-update state is
 * what's queryable.
 *
 * <p>Runs against every metadata-store implementation. Stores without native
 * secondary index support exercise the fallback scan + predicate path inside
 * {@code findByIndex}; native-index stores (Oxia) consult the index directly.
 */
public class MetadataCacheSecondaryIndexTest extends BaseMetadataStoreTest {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class IndexedValue {
        String owner;
        String team;
    }

    /**
     * Predicate used in the fallback path to identify records matching a property.
     * Deserialises the stored bytes to {@link IndexedValue} and checks the field.
     */
    private static java.util.function.Predicate<GetResult> matchOwner(String owner) {
        return result -> {
            try {
                IndexedValue v = com.fasterxml.jackson.databind.json.JsonMapper.builder()
                        .build().readValue(result.getValue(), IndexedValue.class);
                return owner.equals(v.getOwner());
            } catch (Exception e) {
                return false;
            }
        };
    }

    private static java.util.function.Predicate<GetResult> matchTeam(String team) {
        return result -> {
            try {
                IndexedValue v = com.fasterxml.jackson.databind.json.JsonMapper.builder()
                        .build().readValue(result.getValue(), IndexedValue.class);
                return team.equals(v.getTeam());
            } catch (Exception e) {
                return false;
            }
        };
    }

    @Test(dataProvider = "impl")
    public void createWithIndexExtractorRegistersIndexes(String provider,
                                                          Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        MetadataCache<IndexedValue> cache = store.getMetadataCache(IndexedValue.class);

        String basePath = newKey();

        // Two records share an owner; one is on a different owner. The extractor
        // exposes both fields as separate indexes so we can query either one.
        cache.create(basePath + "/r1", new IndexedValue("alice", "platform"),
                v -> Map.of("by-owner", v.getOwner(), "by-team", v.getTeam())).join();
        cache.create(basePath + "/r2", new IndexedValue("alice", "data"),
                v -> Map.of("by-owner", v.getOwner(), "by-team", v.getTeam())).join();
        cache.create(basePath + "/r3", new IndexedValue("bob", "platform"),
                v -> Map.of("by-owner", v.getOwner(), "by-team", v.getTeam())).join();

        // Owner=alice should return r1 + r2.
        List<GetResult> aliceOwned = store.findByIndex(basePath, "by-owner", "alice",
                matchOwner("alice")).join();
        assertEquals(aliceOwned.size(), 2);

        // Team=platform should return r1 + r3.
        Set<String> platformPaths = new HashSet<>();
        for (GetResult r : store.findByIndex(basePath, "by-team", "platform",
                matchTeam("platform")).join()) {
            platformPaths.add(r.getStat().getPath());
        }
        assertEquals(platformPaths, Set.of(basePath + "/r1", basePath + "/r3"));
    }

    @Test(dataProvider = "impl")
    public void readModifyUpdateRefreshesIndexes(String provider,
                                                  Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        MetadataCache<IndexedValue> cache = store.getMetadataCache(IndexedValue.class);

        String basePath = newKey();
        java.util.function.Function<IndexedValue, Map<String, String>> extractor =
                v -> Map.of("by-owner", v.getOwner());

        cache.create(basePath + "/r1", new IndexedValue("alice", "platform"), extractor).join();
        assertEquals(store.findByIndex(basePath, "by-owner", "alice", matchOwner("alice"))
                .join().size(), 1);

        // Reassign owner via update — the new owner becomes the queryable one and the
        // old owner's lookup must no longer surface this record.
        cache.readModifyUpdate(basePath + "/r1", current -> new IndexedValue("bob", current.getTeam()),
                extractor).join();

        assertEquals(store.findByIndex(basePath, "by-owner", "bob", matchOwner("bob"))
                .join().size(), 1);
        assertEquals(store.findByIndex(basePath, "by-owner", "alice", matchOwner("alice"))
                .join().size(), 0);
    }

    @Test(dataProvider = "impl")
    public void emptyExtractorBehavesLikePlainCreate(String provider,
                                                       Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        MetadataCache<IndexedValue> cache = store.getMetadataCache(IndexedValue.class);

        String path = newKey();

        // An extractor returning an empty map must not register any indexes — but the
        // record must still be written and readable.
        cache.create(path, new IndexedValue("alice", "platform"), v -> Map.of()).join();

        assertTrue(cache.get(path).join().isPresent());
        // No index registered, so a lookup with the would-be index name returns nothing.
        assertEquals(store.findByIndex(path, "by-owner", "alice", r -> false).join().size(), 0);
    }
}
