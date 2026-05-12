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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Option;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration tests for {@link Option.PartitionKey} routing on a multi-shard Oxia cluster.
 *
 * <p>These tests verify that the {@link Option.PartitionKey} option is plumbed end-to-end through
 * the {@code MetadataStore} API down into the Oxia client. They run against a 3-shard cluster so
 * routed operations exercise the cross-shard path, but they do not assert which shard a particular
 * key lands in (Oxia's hash function is an implementation detail).
 */
public class OxiaPartitionKeyTest {

    private static final int SHARDS = 3;
    private OxiaContainer oxiaServer;

    @BeforeClass
    public void start() {
        oxiaServer = new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME).withShards(SHARDS);
        oxiaServer.start();
    }

    @AfterClass(alwaysRun = true)
    public void stop() {
        if (oxiaServer != null) {
            oxiaServer.close();
            oxiaServer = null;
        }
    }

    private MetadataStore newStore() throws Exception {
        return MetadataStoreFactory.create(
                "oxia://" + oxiaServer.getServiceAddress(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
    }

    @Test
    public void putAndGetRoundTripWithPartitionKey() throws Exception {
        @Cleanup
        MetadataStore store = newStore();

        String path = "/partition-key-roundtrip-" + System.nanoTime();
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        Set<Option> opts = Set.of(new Option.PartitionKey("group-A"));

        store.put(path, value, Optional.empty(), opts).get();

        var result = store.get(path, opts).get();
        assertTrue(result.isPresent(), "value should be reachable with the same partition key");
        assertEquals(result.get().getValue(), value);
    }

    @Test
    public void deleteUsesPartitionKey() throws Exception {
        @Cleanup
        MetadataStore store = newStore();

        String path = "/partition-key-delete-" + System.nanoTime();
        Set<Option> opts = Set.of(new Option.PartitionKey("group-X"));

        store.put(path, "v".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();
        assertTrue(store.get(path, opts).get().isPresent());

        store.delete(path, Optional.empty(), opts).get();
        assertFalse(store.get(path, opts).get().isPresent());
    }

    @Test
    public void getChildrenWithPartitionKey() throws Exception {
        @Cleanup
        MetadataStore store = newStore();

        String parent = "/partition-key-children-" + System.nanoTime();
        Set<Option> opts = Set.of(new Option.PartitionKey("group-Y"));

        store.put(parent + "/a", "1".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();
        store.put(parent + "/b", "2".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();

        List<String> children = store.getChildren(parent, opts).get();
        assertTrue(children.containsAll(List.of("a", "b")),
                "expected children a and b, got: " + children);
    }
}
