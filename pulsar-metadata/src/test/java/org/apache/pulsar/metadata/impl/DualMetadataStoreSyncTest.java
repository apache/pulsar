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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.common.migration.MigrationPhase;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "metadata")
public class DualMetadataStoreSyncTest {

    @DataProvider(name = "phases")
    public Object[][] phases() {
        return Arrays.stream(MigrationPhase.values()).map(phase -> new Object[]{phase}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "phases", timeOut = 30000)
    public void syncUsesCurrentReadStoreAndPropagatesItsBarrierAndFailure(MigrationPhase phase) throws Exception {
        MetadataStoreConfig config = MetadataStoreConfig.builder().build();
        @Cleanup
        LocalMemoryMetadataStore source = spy(new LocalMemoryMetadataStore("memory:local", config));
        @Cleanup
        LocalMemoryMetadataStore target = spy(new LocalMemoryMetadataStore("memory:local", config));
        String path = "/sync-read";
        source.put(path, "source".getBytes(UTF_8), Optional.empty()).get(5, TimeUnit.SECONDS);
        target.put(path, "target".getBytes(UTF_8), Optional.empty()).get(5, TimeUnit.SECONDS);
        String targetUrl = "sync-test:" + UUID.randomUUID();
        // Join the persisted migration phase through the normal constructor, without changing wrapper state.
        source.put(MigrationState.MIGRATION_FLAG_PATH, ObjectMapperFactory.getMapper().writer()
                .writeValueAsBytes(new MigrationState(phase, targetUrl)), Optional.empty()).get(5, TimeUnit.SECONDS);
        String originalProviders = System.getProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY);
        String providers = SyncMetadataStoreProvider.class.getName();
        if (originalProviders != null && !originalProviders.isBlank()) {
            providers = originalProviders + "," + providers;
        }
        System.setProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY, providers);
        SyncMetadataStoreProvider.STORES.put(targetUrl, target);
        try {
            @Cleanup
            DualMetadataStore store = new DualMetadataStore(source, config);
            assertThat(store.getMigrationPhase()).isEqualTo(phase);
            if (phase == MigrationPhase.PREPARATION || phase == MigrationPhase.COPYING) {
                // Let the real preparation handler finish before closing its stores.
                await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(source
                        .getChildrenFromStore(MigrationState.PARTICIPANTS_PATH).get(5, TimeUnit.SECONDS)).isEmpty());
            }

            MetadataStoreExtended readStore = phase == MigrationPhase.COMPLETED ? target : source;
            MetadataStoreExtended otherStore = phase == MigrationPhase.COMPLETED ? source : target;
            assertThat(store.get(path).get(5, TimeUnit.SECONDS).orElseThrow().getValue())
                    .isEqualTo((phase == MigrationPhase.COMPLETED ? "target" : "source").getBytes(UTF_8));
            CompletableFuture<Void> barrier = new CompletableFuture<>();
            doReturn(barrier).when(readStore).sync(path);
            CompletableFuture<Void> sync = store.sync(path);
            assertThat(sync).isNotDone();
            barrier.complete(null);
            sync.get(5, TimeUnit.SECONDS);

            MetadataStoreException failure = new MetadataStoreException("read backend sync unavailable");
            doReturn(CompletableFuture.failedFuture(failure)).when(readStore).sync(path);
            assertThatThrownBy(() -> store.sync(path).get(5, TimeUnit.SECONDS)).hasCause(failure);
            verify(readStore, times(2)).sync(path);
            verify(otherStore, never()).sync(path);
        } finally {
            SyncMetadataStoreProvider.STORES.remove(targetUrl);
            if (originalProviders == null) {
                System.clearProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY);
            } else {
                System.setProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY, originalProviders);
            }
        }
    }

    /** Supplies a controlled target through the production migration provider lookup. */
    public static class SyncMetadataStoreProvider implements MetadataStoreProvider {
        private static final Map<String, MetadataStore> STORES = new ConcurrentHashMap<>();

        @Override
        public String urlScheme() {
            return "sync-test";
        }

        @Override
        public MetadataStore create(String metadataURL, MetadataStoreConfig metadataStoreConfig,
                                    boolean enableSessionWatcher) throws MetadataStoreException {
            MetadataStore store = STORES.get(metadataURL);
            if (store == null) {
                throw new MetadataStoreException("No test store registered for " + metadataURL);
            }
            return store;
        }
    }
}
