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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Optional;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.migration.MigrationPhase;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.MigrationCoordinator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MigrationCoordinatorTest extends BaseMetadataStoreTest {

    protected String getOxiaServerConnectString() {
        return "oxia://" + super.getOxiaServerConnectString();
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        super.setup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testPersistentDataCopy() throws Exception {
        String prefix = newKey();

        @Cleanup
        MetadataStoreExtended sourceStore =
                (MetadataStoreExtended) MetadataStoreFactory.create(zks.getConnectionString(),
                        MetadataStoreConfig.builder().build());

        String targetUrl = getOxiaServerConnectString();

        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(targetUrl, MetadataStoreConfig.builder().build());

        // Create persistent nodes
        String key1 = prefix + "/persistent/key1";
        String key2 = prefix + "/persistent/key2";
        String key3 = prefix + "/persistent/nested/key3";

        sourceStore.put(key1, "value1".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        sourceStore.put(key2, "value2".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        sourceStore.put(key3, "value3".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();

        // Create ephemeral node (should NOT be copied)
        String ephemeralKey = prefix + "/ephemeral/key";
        sourceStore.put(ephemeralKey, "ephemeral-value".getBytes(StandardCharsets.UTF_8),
                Optional.empty(), EnumSet.of(CreateOption.Ephemeral)).join();

        // Start migration
        MigrationCoordinator coordinator = new MigrationCoordinator(sourceStore, targetUrl);
        coordinator.startMigration();

        Optional<GetResult> result = sourceStore.get(MigrationState.MIGRATION_FLAG_PATH).join();
        assertTrue(result.isPresent());
        MigrationState state = ObjectMapperFactory.getMapper().reader()
                .readValue(result.get().getValue(), MigrationState.class);
        assertEquals(state.getPhase(), MigrationPhase.COMPLETED);

        // Verify persistent nodes were copied
        Optional<GetResult> target1 = targetStore.get(key1).join();
        assertTrue(target1.isPresent());
        assertEquals(new String(target1.get().getValue(), StandardCharsets.UTF_8), "value1");

        Optional<GetResult> target2 = targetStore.get(key2).join();
        assertTrue(target2.isPresent());
        assertEquals(new String(target2.get().getValue(), StandardCharsets.UTF_8), "value2");

        Optional<GetResult> target3 = targetStore.get(key3).join();
        assertTrue(target3.isPresent());
        assertEquals(new String(target3.get().getValue(), StandardCharsets.UTF_8), "value3");

        // Verify ephemeral node is in the target store
        Optional<GetResult> targetEphemeral = targetStore.get(ephemeralKey).join();
        assertTrue(targetEphemeral.isPresent());
        assertEquals(new String(targetEphemeral.get().getValue(), StandardCharsets.UTF_8), "ephemeral-value");
    }

    @Test
    public void testVersionPreservation() throws Exception {
        String prefix = newKey();

        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String targetUrl = getOxiaServerConnectString();

        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(targetUrl,
                MetadataStoreConfig.builder().build());

        // Create a node and update it multiple times to get a specific version
        String key = prefix + "/versioned-key";
        sourceStore.put(key, "v1".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        sourceStore.put(key, "v2".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        sourceStore.put(key, "v3".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();

        // Get the version from source
        Optional<GetResult> sourceResult = sourceStore.get(key).join();
        assertTrue(sourceResult.isPresent());
        long sourceVersion = sourceResult.get().getStat().getVersion();

        // Start migration
        MigrationCoordinator coordinator = new MigrationCoordinator(sourceStore, targetUrl);
        coordinator.startMigration();

        Optional<GetResult> result = sourceStore.get(MigrationState.MIGRATION_FLAG_PATH).join();
        assertTrue(result.isPresent());
        MigrationState state = ObjectMapperFactory.getMapper().reader()
                .readValue(result.get().getValue(), MigrationState.class);
        assertEquals(state.getPhase(), MigrationPhase.COMPLETED);

        // Verify version and modification count were preserved in target
        Optional<GetResult> targetResult = targetStore.get(key).join();
        assertTrue(targetResult.isPresent());
        assertEquals(targetResult.get().getStat().getVersion(), sourceVersion);
        assertEquals(new String(targetResult.get().getValue(), StandardCharsets.UTF_8), "v3");
    }

    @Test
    public void testEmptyMetadataMigration() throws Exception {
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().build());

        String targetUrl = getOxiaServerConnectString();

        // Start migration with empty metadata
        MigrationCoordinator coordinator = new MigrationCoordinator(sourceStore, targetUrl);
        coordinator.startMigration();

        Optional<GetResult> result = sourceStore.get(MigrationState.MIGRATION_FLAG_PATH).join();
        assertTrue(result.isPresent());
        MigrationState state = ObjectMapperFactory.getMapper().reader()
                .readValue(result.get().getValue(), MigrationState.class);
        assertEquals(state.getPhase(), MigrationPhase.COMPLETED);
    }

    @Test
    public void testLargeDatasetMigration() throws Exception {
        String prefix = newKey();

        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String targetUrl = getOxiaServerConnectString();

        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(targetUrl,
                MetadataStoreConfig.builder().build());

        // Create a larger dataset (100 nodes)
        int nodeCount = 100;
        for (int i = 0; i < nodeCount; i++) {
            String key = prefix + "/data/node-" + i;
            String value = "value-" + i;
            sourceStore.put(key, value.getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        }

        long startTime = System.currentTimeMillis();

        // Start migration
        MigrationCoordinator coordinator = new MigrationCoordinator(sourceStore, targetUrl);
        coordinator.startMigration();

        Optional<GetResult> result = sourceStore.get(MigrationState.MIGRATION_FLAG_PATH).join();
        assertTrue(result.isPresent());
        MigrationState state = ObjectMapperFactory.getMapper().reader()
                .readValue(result.get().getValue(), MigrationState.class);
        assertEquals(state.getPhase(), MigrationPhase.COMPLETED);

        long duration = System.currentTimeMillis() - startTime;
        log.info("Migration of {} nodes completed in {} ms", nodeCount, duration);

        // Verify all nodes were copied
        for (int i = 0; i < nodeCount; i++) {
            String key = prefix + "/data/node-" + i;
            Optional<GetResult> targetResult = targetStore.get(key).join();
            assertTrue(targetResult.isPresent(), "Node " + key + " should exist in target");
            assertEquals(new String(targetResult.get().getValue(), StandardCharsets.UTF_8),
                    "value-" + i);
        }
    }

    @Test
    public void testNestedPathMigration() throws Exception {
        String prefix = newKey();

        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String targetUrl = getOxiaServerConnectString();

        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(targetUrl,
                MetadataStoreConfig.builder().build());

        // Create nested paths
        sourceStore.put(prefix + "/level1/key1", "value1".getBytes(StandardCharsets.UTF_8),
                Optional.empty()).join();
        sourceStore.put(prefix + "/level1/level2/key2", "value2".getBytes(StandardCharsets.UTF_8),
                Optional.empty()).join();
        sourceStore.put(prefix + "/level1/level2/level3/key3",
                "value3".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();

        // Start migration
        MigrationCoordinator coordinator = new MigrationCoordinator(sourceStore, targetUrl);
        coordinator.startMigration();

        Optional<GetResult> result = sourceStore.get(MigrationState.MIGRATION_FLAG_PATH).join();
        assertTrue(result.isPresent());
        MigrationState state = ObjectMapperFactory.getMapper().reader()
                .readValue(result.get().getValue(), MigrationState.class);
        assertEquals(state.getPhase(), MigrationPhase.COMPLETED);

        // Verify all nested paths were copied
        Optional<GetResult> target1 = targetStore.get(prefix + "/level1/key1").join();
        assertTrue(target1.isPresent());
        assertEquals(new String(target1.get().getValue(), StandardCharsets.UTF_8), "value1");

        Optional<GetResult> target2 = targetStore.get(prefix + "/level1/level2/key2").join();
        assertTrue(target2.isPresent());
        assertEquals(new String(target2.get().getValue(), StandardCharsets.UTF_8), "value2");

        Optional<GetResult> target3 = targetStore.get(prefix + "/level1/level2/level3/key3").join();
        assertTrue(target3.isPresent());
        assertEquals(new String(target3.get().getValue(), StandardCharsets.UTF_8), "value3");
    }

    @Test
    public void testMigrationStateStructure() throws Exception {
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String targetUrl = getOxiaServerConnectString();

        // Start migration
        MigrationCoordinator coordinator = new MigrationCoordinator(sourceStore, targetUrl);
        coordinator.startMigration();

        // Verify migration state structure
        Optional<GetResult> result = sourceStore.get(MigrationState.MIGRATION_FLAG_PATH).join();
        assertTrue(result.isPresent());

        MigrationState state = ObjectMapperFactory.getMapper().reader()
                .readValue(result.get().getValue(), MigrationState.class);

        assertNotNull(state.getPhase());
        assertNotNull(state.getTargetUrl());
        assertEquals(state.getTargetUrl(), targetUrl);

        // Phase should be PREPARATION or COPYING or COMPLETED
        assertTrue(state.getPhase() == MigrationPhase.PREPARATION
                || state.getPhase() == MigrationPhase.COPYING
                || state.getPhase() == MigrationPhase.COMPLETED);
    }
}
