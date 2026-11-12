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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests that read batches survive the multiRead limits of a real ZooKeeper server.
 */
public class ZKMetadataStoreMultiReadLimitsTest extends BaseMetadataStoreTest {
    private static final int DEFAULT_MAX_OPS = 1000;
    private static final long DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024;
    private static final int TIMEOUT_SECONDS = 30;

    @AfterMethod(alwaysRun = true)
    public void resetLimits() {
        if (zks != null) {
            zks.setMultiReadLimits(DEFAULT_MAX_OPS, DEFAULT_MAX_RESPONSE_BYTES);
        }
    }

    @Test
    public void testBatchOverResponseLimitIsSplit() throws Exception {
        // each read fits within the limit, but the batch of 5 reads doesn't
        int valueSize = 40 * 1024;
        List<String> keys = createNodes(6, valueSize);
        zks.setMultiReadLimits(DEFAULT_MAX_OPS, 64 * 1024);
        @Cleanup
        MetadataStore store = createBatchingStore(5);

        // the 6th read exceeds the maximum of 5 queued operations and triggers the flush
        for (CompletableFuture<Optional<GetResult>> future : getAll(store, keys)) {
            assertThat(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isPresent()
                    .hasValueSatisfying(result -> assertThat(result.getValue()).hasSize(valueSize));
        }
    }

    @Test
    public void testBatchOverOperationCountLimitIsSplit() throws Exception {
        String parent = newKey();
        List<String> keys = createNodes(parent, 3, 1);
        String missingKey = parent + "/missing";
        zks.setMultiReadLimits(2, DEFAULT_MAX_RESPONSE_BYTES);
        @Cleanup
        MetadataStore store = createBatchingStore(5);

        CompletableFuture<List<String>> children = store.getChildren(parent);
        List<CompletableFuture<Optional<GetResult>>> existing = getAll(store, keys);
        CompletableFuture<Optional<GetResult>> missing = store.get(missingKey);
        // the 6th operation exceeds the maximum of 5 queued operations and triggers the flush
        CompletableFuture<Optional<GetResult>> trigger = store.get(keys.get(0));

        assertThat(children.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).hasSize(3);
        for (CompletableFuture<Optional<GetResult>> future : existing) {
            assertThat(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isPresent();
        }
        assertThat(missing.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEmpty();
        assertThat(trigger.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isPresent();
    }

    @Test
    public void testSingleReadOverResponseLimitFailsAlone() throws Exception {
        List<String> smallKeys = createNodes(3, 1);
        String largeKey = createNodes(1, 100 * 1024).get(0);
        zks.setMultiReadLimits(DEFAULT_MAX_OPS, 64 * 1024);
        @Cleanup
        MetadataStore store = createBatchingStore(3);

        CompletableFuture<Optional<GetResult>> small1 = store.get(smallKeys.get(0));
        CompletableFuture<Optional<GetResult>> large = store.get(largeKey);
        CompletableFuture<Optional<GetResult>> small2 = store.get(smallKeys.get(1));
        // the 4th operation exceeds the maximum of 3 queued operations and triggers the flush
        CompletableFuture<Optional<GetResult>> small3 = store.get(smallKeys.get(2));

        assertThatThrownBy(() -> large.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(KeeperException.BadArgumentsException.class);
        assertThat(small1.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isPresent();
        assertThat(small2.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isPresent();
        assertThat(small3.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isPresent();
    }

    /**
     * Creates a store whose periodic flush is slow enough that reads queued right after its creation flush only when
     * more than maxOperations are queued, so the first maxOperations reads form a single batch. Creating the store
     * reads through it, so the delay also determines how long the creation takes.
     */
    private MetadataStore createBatchingStore(int maxOperations) throws Exception {
        return MetadataStoreFactory.create(zks.getConnectionString(), MetadataStoreConfig.builder()
                .batchingEnabled(true)
                .batchingMaxDelayMillis(2_000)
                .batchingMaxOperations(maxOperations)
                .build());
    }

    private List<String> createNodes(int count, int valueSize) throws Exception {
        return createNodes(newKey(), count, valueSize);
    }

    private List<String> createNodes(String parent, int count, int valueSize) throws Exception {
        @Cleanup
        MetadataStore writer = MetadataStoreFactory.create(zks.getConnectionString(), MetadataStoreConfig.builder()
                .batchingEnabled(false)
                .build());
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String key = parent + "/node-" + i;
            writer.put(key, new byte[valueSize], Optional.empty()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            keys.add(key);
        }
        return keys;
    }

    private static List<CompletableFuture<Optional<GetResult>>> getAll(MetadataStore store, List<String> keys) {
        List<CompletableFuture<Optional<GetResult>>> futures = new ArrayList<>();
        for (String key : keys) {
            futures.add(store.get(key));
        }
        return futures;
    }
}
