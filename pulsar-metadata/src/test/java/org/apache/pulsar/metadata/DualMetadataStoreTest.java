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
import static org.testng.Assert.fail;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.migration.MigrationPhase;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.DualMetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class DualMetadataStoreTest extends BaseMetadataStoreTest {


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
    public void testNotStartedPhaseRoutesToSource() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        // Write should go to source store
        String path = prefix + "/test-key";
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        dualStore.put(path, data, Optional.empty()).join();

        // Verify data in source store
        Optional<GetResult> result = sourceStore.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(new String(result.get().getValue(), StandardCharsets.UTF_8), "test-data");

        // Read should come from source store
        Optional<GetResult> readResult = dualStore.get(path).join();
        assertTrue(readResult.isPresent());
        assertEquals(new String(readResult.get().getValue(), StandardCharsets.UTF_8), "test-data");
    }

    @Test
    public void testPreparationPhaseBlocksWrites() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        // Set migration state to PREPARATION
        MigrationState preparationState = new MigrationState(MigrationPhase.PREPARATION,
                "memory:" + UUID.randomUUID());
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(preparationState),
                Optional.empty()).join();

        // Wait for dual store to detect migration
        Thread.sleep(500);

        // Writes should be blocked
        String path = prefix + "/test-key";
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        try {
            dualStore.put(path, data, Optional.empty()).join();
            fail("Should have thrown IllegalStateException");
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
            assertTrue(e.getCause().getMessage().contains("Write operations not allowed during migrations"));
        }

        // Reads should still work
        Optional<GetResult> result = dualStore.get(path).join();
        assertFalse(result.isPresent());
    }

    @Test
    public void testCopyingPhaseBlocksWrites() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        // Set migration state to COPYING
        MigrationState copyingState = new MigrationState(MigrationPhase.COPYING,
                "memory:" + UUID.randomUUID());
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(copyingState),
                Optional.empty()).join();

        // Wait for dual store to detect migration
        Thread.sleep(500);

        // Writes should be blocked
        String path = prefix + "/test-key";
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        try {
            dualStore.put(path, data, Optional.empty()).join();
            fail("Should have thrown IllegalStateException");
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
            assertTrue(e.getCause().getMessage().contains("Write operations not allowed during migration"));
        }
    }

    @Test
    public void testCompletedPhaseRoutesToTarget() throws Exception {
        String prefix = newKey();

        @Cleanup
        MetadataStore store =
                MetadataStoreFactory.create(zks.getConnectionString(), MetadataStoreConfig.builder().build());

        String oxiaService = "oxia://" + getOxiaServerConnectString();

        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(oxiaService,
                MetadataStoreConfig.builder().fsyncEnable(false).build());


        // Set migration state to PREPARATION
        MigrationState preparationState = new MigrationState(MigrationPhase.PREPARATION, oxiaService);
        store.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(preparationState),
                Optional.empty()).join();

        // Set migration state to COMPLETED
        MigrationState completedState = new MigrationState(MigrationPhase.COMPLETED, oxiaService);
        store.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(completedState),
                Optional.empty()).join();

        // Wait for dual store to detect migration and initialize target
        Thread.sleep(1000);

        // Write should go to target store
        String path = prefix + "/test-key";
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        store.put(path, data, Optional.empty()).join();

        // Verify data in target store
        Optional<GetResult> targetResult = targetStore.get(path).join();
        assertTrue(targetResult.isPresent());
        assertEquals(new String(targetResult.get().getValue(), StandardCharsets.UTF_8), "test-data");

        // Read should come from target store
        Optional<GetResult> readResult = store.get(path).join();
        assertTrue(readResult.isPresent());
        assertEquals(new String(readResult.get().getValue(), StandardCharsets.UTF_8), "test-data");
    }

    @Test
    public void testFailedPhaseRoutesToSource() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        // Set migration state to FAILED
        MigrationState failedState = new MigrationState(MigrationPhase.FAILED,
                "memory:" + UUID.randomUUID());
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(failedState),
                Optional.empty()).join();

        // Wait for dual store to detect migration
        Thread.sleep(500);

        // Write should go to source store (migration failed)
        String path = prefix + "/test-key";
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        dualStore.put(path, data, Optional.empty()).join();

        // Verify data in source store
        Optional<GetResult> result = sourceStore.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(new String(result.get().getValue(), StandardCharsets.UTF_8), "test-data");
    }

    @Test
    public void testSessionLostEventDuringPreparation() throws Exception {
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        CountDownLatch sessionLostLatch = new CountDownLatch(1);
        AtomicReference<SessionEvent> receivedEvent = new AtomicReference<>();

        dualStore.registerSessionListener(event -> {
            receivedEvent.set(event);
            if (event == SessionEvent.SessionLost) {
                sessionLostLatch.countDown();
            }
        });

        // Set migration state to PREPARATION to trigger SessionLost
        MigrationState preparationState = new MigrationState(MigrationPhase.PREPARATION,
                "memory:" + UUID.randomUUID());
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(preparationState),
                Optional.empty()).join();

        // Wait for SessionLost event
        assertTrue(sessionLostLatch.await(5, TimeUnit.SECONDS));
        assertEquals(receivedEvent.get(), SessionEvent.SessionLost);
    }

    @Test
    public void testSessionReestablishedEventOnCompletion() throws Exception {
        @Cleanup
        MetadataStore sourceStore =
                new ZKMetadataStore(zks.getConnectionString(), MetadataStoreConfig.builder().build(), true);

        String targetUrl = "memory:" + UUID.randomUUID();

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        CountDownLatch sessionReestablishedLatch = new CountDownLatch(1);
        List<SessionEvent> receivedEvents = new ArrayList<>();

        dualStore.registerSessionListener(event -> {
            receivedEvents.add(event);
            if (event == SessionEvent.SessionReestablished) {
                sessionReestablishedLatch.countDown();
            }
        });

        // First trigger PREPARATION (SessionLost)
        MigrationState preparationState = new MigrationState(MigrationPhase.PREPARATION, targetUrl);
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(preparationState),
                Optional.empty()).join();

        Thread.sleep(500);

        // Then trigger COMPLETED (SessionReestablished)
        MigrationState completedState = new MigrationState(MigrationPhase.COMPLETED, targetUrl);
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(completedState),
                Optional.empty()).join();

        // Wait for SessionReestablished event
        assertTrue(sessionReestablishedLatch.await(5, TimeUnit.SECONDS));
        assertTrue(receivedEvents.contains(SessionEvent.SessionLost));
        assertTrue(receivedEvents.contains(SessionEvent.SessionReestablished));
    }

    @Test
    public void testSessionReestablishedEventOnFailure() throws Exception {
        MetadataStore zkStore =
                new ZKMetadataStore(zks.getConnectionString(), MetadataStoreConfig.builder().build(), true);

        @Cleanup
        MetadataStoreExtended dualStore = new DualMetadataStore(zkStore, MetadataStoreConfig.builder().build());

        CountDownLatch sessionReestablishedLatch = new CountDownLatch(1);
        List<SessionEvent> receivedEvents = new ArrayList<>();

        dualStore.registerSessionListener(event -> {
            receivedEvents.add(event);
            if (event == SessionEvent.SessionReestablished) {
                sessionReestablishedLatch.countDown();
            }
        });

        // First trigger PREPARATION (SessionLost)
        MigrationState preparationState = new MigrationState(MigrationPhase.PREPARATION,
                "memory:" + UUID.randomUUID());
        zkStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(preparationState),
                Optional.empty()).join();

        Thread.sleep(500);

        // Then trigger FAILED (SessionReestablished)
        MigrationState failedState = new MigrationState(MigrationPhase.FAILED,
                "memory:" + UUID.randomUUID());
        zkStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(failedState),
                Optional.empty()).join();

        // Wait for SessionReestablished event
        assertTrue(sessionReestablishedLatch.await(5, TimeUnit.SECONDS));
        assertTrue(receivedEvents.contains(SessionEvent.SessionLost));
        assertTrue(receivedEvents.contains(SessionEvent.SessionReestablished));
    }

    @Test
    public void testParticipantRegistration() throws Exception {
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        // Verify participant registration node exists
        List<String> participants = sourceStore.getChildren(MigrationState.PARTICIPANTS_PATH).join();
        log.info("participants: {}", participants);
        assertEquals(participants.size(), 1);
        assertTrue(participants.get(0).startsWith("id-"));
    }

    @Test
    public void testDeleteOperationRouting() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore dualStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        assertEquals(dualStore.getClass(), DualMetadataStore.class);

        // Create a key in NOT_STARTED phase
        String path = prefix + "/test-key";
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        dualStore.put(path, data, Optional.empty()).join();

        // Delete should work in NOT_STARTED phase
        dualStore.delete(path, Optional.empty()).join();
        assertFalse(dualStore.exists(path).join());
    }

    @Test
    public void testExistsOperationRouting() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), false);

        String targetUrl = "memory:" + UUID.randomUUID();
        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(targetUrl,
                MetadataStoreConfig.builder().build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        String path = prefix + "/test-key";
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);

        // Create in source
        sourceStore.put(path, data, Optional.empty()).join();

        // Exists should check source in NOT_STARTED phase
        assertTrue(dualStore.exists(path).join());

        // First trigger PREPARATION (SessionLost)
        MigrationState preparationState = new MigrationState(MigrationPhase.PREPARATION, targetUrl);
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(preparationState),
                Optional.empty()).join();

        // Switch to COMPLETED phase
        MigrationState completedState = new MigrationState(MigrationPhase.COMPLETED, targetUrl);
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(completedState),
                Optional.empty()).join();

        Thread.sleep(1000);

        // Create in target
        String targetPath = prefix + "/target-key";
        targetStore.put(targetPath, data, Optional.empty()).join();

        // Exists should check target in COMPLETED phase
        assertTrue(dualStore.exists(targetPath).join());
    }
}
