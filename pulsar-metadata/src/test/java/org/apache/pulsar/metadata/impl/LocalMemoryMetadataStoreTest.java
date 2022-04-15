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
package org.apache.pulsar.metadata.impl;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class LocalMemoryMetadataStoreTest {

    @Test
    public void testPrivateInstance() throws Exception {
        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().build());

        @Cleanup
        MetadataStore store2 = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().build());

        store1.put("/test", "value".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();

        assertTrue(store1.exists("/test").join());
        assertFalse(store2.exists("/test").join());
    }

    @Test
    public void testSharedInstance() throws Exception {
        String url = "memory:" + UUID.randomUUID();

        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create(url,
                MetadataStoreConfig.builder().build());

        @Cleanup
        MetadataStore store2 = MetadataStoreFactory.create(url,
                MetadataStoreConfig.builder().build());

        store1.put("/test", "value".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();

        assertTrue(store1.exists("/test").join());
        assertTrue(store2.exists("/test").join());

        store2.delete("/test", Optional.empty()).join();

        assertFalse(store2.exists("/test").join());

        // The exists will be updated based on the cache invalidation in store1
        Awaitility.await().untilAsserted(() -> {
            assertFalse(store1.exists("/test").join());
        });
    }

    @Test
    public void testPathValid() {
        assertFalse(AbstractMetadataStore.isValidPath(null));
        assertFalse(AbstractMetadataStore.isValidPath(""));
        assertFalse(AbstractMetadataStore.isValidPath(" "));
        assertTrue(AbstractMetadataStore.isValidPath("/"));
        assertTrue(AbstractMetadataStore.isValidPath("/test"));
        assertFalse(AbstractMetadataStore.isValidPath("/test/"));
        assertTrue(AbstractMetadataStore.isValidPath("/test/ABC"));
    }
}
