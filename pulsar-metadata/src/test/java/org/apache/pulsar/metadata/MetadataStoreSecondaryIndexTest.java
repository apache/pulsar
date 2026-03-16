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
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class MetadataStoreSecondaryIndexTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void putWithSecondaryIndexesPreservesValue(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);

        store.put(path, value, Optional.of(-1L), EnumSet.noneOf(CreateOption.class),
                Map.of("by-owner", "broker-1")).join();

        var result = store.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getValue(), value);
    }

    @Test(dataProvider = "impl")
    public void putWithMultipleSecondaryIndexes(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();
        byte[] value = "multi-index-value".getBytes(StandardCharsets.UTF_8);

        store.put(path, value, Optional.of(-1L), EnumSet.noneOf(CreateOption.class),
                Map.of("by-owner", "broker-1", "by-namespace", "tenant/ns1")).join();

        var result = store.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getValue(), value);
    }

    @Test(dataProvider = "impl")
    public void putWithEmptySecondaryIndexes(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();
        byte[] value = "no-index-value".getBytes(StandardCharsets.UTF_8);

        store.put(path, value, Optional.of(-1L), EnumSet.noneOf(CreateOption.class), Map.of()).join();

        var result = store.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getValue(), value);
    }

    @Test(dataProvider = "impl")
    public void findByIndexFallbackReturnsFilteredResults(String provider, Supplier<String> urlSupplier)
            throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String basePath = newKey();

        store.put(basePath + "/topic-1", "owned-by-broker-1".getBytes(StandardCharsets.UTF_8),
                Optional.of(-1L), EnumSet.noneOf(CreateOption.class),
                Map.of("by-owner", "broker-1")).join();
        store.put(basePath + "/topic-2", "owned-by-broker-2".getBytes(StandardCharsets.UTF_8),
                Optional.of(-1L), EnumSet.noneOf(CreateOption.class),
                Map.of("by-owner", "broker-2")).join();
        store.put(basePath + "/topic-3", "owned-by-broker-1".getBytes(StandardCharsets.UTF_8),
                Optional.of(-1L), EnumSet.noneOf(CreateOption.class),
                Map.of("by-owner", "broker-1")).join();

        List<GetResult> results = store.findByIndex(basePath, "by-owner", "broker-1",
                r -> new String(r.getValue(), StandardCharsets.UTF_8).contains("broker-1")).join();

        assertEquals(results.size(), 2);
        Set<String> values = results.stream()
                .map(r -> new String(r.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.toSet());
        assertTrue(values.contains("owned-by-broker-1"));
    }

    @Test(dataProvider = "impl")
    public void findByIndexFallbackWithNoMatches(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String basePath = newKey();

        store.put(basePath + "/topic-1", "value-1".getBytes(StandardCharsets.UTF_8),
                Optional.of(-1L), EnumSet.noneOf(CreateOption.class)).join();

        List<GetResult> results = store.findByIndex(basePath, "by-owner", "nonexistent",
                r -> false).join();

        assertEquals(results.size(), 0);
    }

    @Test(dataProvider = "impl")
    public void findByIndexFallbackWithEmptyPrefix(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String basePath = newKey();

        List<GetResult> results = store.findByIndex(basePath, "by-owner", "broker-1",
                r -> true).join();

        assertEquals(results.size(), 0);
    }

    @Test(dataProvider = "impl")
    public void updateWithSecondaryIndexes(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();

        var stat = store.put(path, "v1".getBytes(StandardCharsets.UTF_8),
                Optional.of(-1L), EnumSet.noneOf(CreateOption.class),
                Map.of("idx", "key-1")).join();

        store.put(path, "v2".getBytes(StandardCharsets.UTF_8),
                Optional.of(stat.getVersion()), EnumSet.noneOf(CreateOption.class),
                Map.of("idx", "key-2")).join();

        var result = store.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getValue(), "v2".getBytes(StandardCharsets.UTF_8));
    }
}
