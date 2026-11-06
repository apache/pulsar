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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.migration.MigrationPhase;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.tableview.impl.MetadataStoreTableViewImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "metadata")
public class DualMetadataStoreLifecycleTest {
    @DataProvider
    public Object[][] listenerTypes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "listenerTypes")
    public void testListenerRegistrationAcrossMigrationCompletion(boolean sessionListener) throws Exception {
        var config = MetadataStoreConfig.builder().build();
        var source = new LocalMemoryMetadataStore("memory:local", config);
        try (var dual = new PausingDualMetadataStore(source, config)) {
            String targetUrl = "memory:" + UUID.randomUUID();
            setMigrationPhase(source, MigrationPhase.PREPARATION, targetUrl);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(source.getChildren(MigrationState.PARTICIPANTS_PATH).join()).isEmpty());
            setMigrationPhase(source, MigrationPhase.COPYING, targetUrl);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> dual.getMigrationPhase() == MigrationPhase.COPYING);
            var events = new LinkedBlockingQueue<Object>();
            var registration = new CompletableFuture<Runnable>();
            var thread = new Thread(() -> {
                dual.registrationThread = Thread.currentThread();
                try {
                    registration.complete(sessionListener
                            ? dual.registerCancellableSessionListener(events::add)
                            : dual.registerCancellableListener(events::add));
                } catch (Throwable error) {
                    registration.completeExceptionally(error);
                }
            }, "metadata-listener-registration");
            var migrationComplete = new CountDownLatch(1);
            dual.registerCancellableSessionListener(event -> {
                if (event == SessionEvent.SessionReestablished) {
                    migrationComplete.countDown();
                }
            });
            thread.start();
            try {
                assertThat(dual.phaseRead.await(10, TimeUnit.SECONDS)).isTrue();
                setMigrationPhase(source, MigrationPhase.COMPLETED, targetUrl);
                assertThat(migrationComplete.await(10, TimeUnit.SECONDS)).isTrue();
            } finally {
                dual.resumeRegistration.countDown();
                thread.join(TimeUnit.SECONDS.toMillis(10));
            }
            assertThat(thread.isAlive()).isFalse();
            Runnable cancel = registration.get(10, TimeUnit.SECONDS);
            var target = (LocalMemoryMetadataStore) dual.targetStore;
            int registeredCount = sessionListener ? target.getSessionListenerCount() : target.getListenerCount();
            if (sessionListener) {
                target.receivedSessionEvent(SessionEvent.ConnectionLost);
                assertThat(events.poll(10, TimeUnit.SECONDS)).isEqualTo(SessionEvent.ConnectionLost);
            } else {
                dual.put("/after-migration", new byte[0], Optional.empty()).get(10, TimeUnit.SECONDS);
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(events).anyMatch(event -> event instanceof Notification notification
                                && notification.getPath().equals("/after-migration")));
            }
            cancel.run();
            cancel.run();
            assertThat(sessionListener ? target.getSessionListenerCount() : target.getListenerCount())
                    .isEqualTo(registeredCount - 1);
        }
    }

    @Test(dataProvider = "listenerTypes")
    public void testListenerRegistrationBeforeTargetInitialization(boolean sessionListener) throws Exception {
        var config = MetadataStoreConfig.builder().build();
        var source = new LocalMemoryMetadataStore("memory:local", config);
        try (var dual = new DualMetadataStore(source, config)) {
            var migrationComplete = new CountDownLatch(1);
            dual.registerCancellableSessionListener(event -> {
                if (event == SessionEvent.SessionReestablished) {
                    migrationComplete.countDown();
                }
            });
            var events = new LinkedBlockingQueue<Object>();
            Runnable cancel;
            try {
                // Target initialization uses this monitor. Hold it while the notification publishes COMPLETED.
                synchronized (dual) {
                    completeMigration(source);
                    Awaitility.await().atMost(10, TimeUnit.SECONDS)
                            .until(() -> dual.getMigrationPhase() == MigrationPhase.COMPLETED);
                    assertThat(dual.targetStore).isNull();
                    cancel = sessionListener ? dual.registerCancellableSessionListener(events::add)
                            : dual.registerCancellableListener(events::add);
                }
            } finally {
                assertThat(migrationComplete.await(10, TimeUnit.SECONDS)).isTrue();
            }
            var target = (LocalMemoryMetadataStore) dual.targetStore;
            if (sessionListener) {
                target.receivedSessionEvent(SessionEvent.ConnectionLost);
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(events).contains(SessionEvent.ConnectionLost));
            } else {
                dual.put("/after-target-init", new byte[0], Optional.empty()).get(10, TimeUnit.SECONDS);
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(events).anyMatch(event -> event instanceof Notification notification
                                && notification.getPath().equals("/after-target-init")));
            }
            int registeredCount = sessionListener ? target.getSessionListenerCount() : target.getListenerCount();
            cancel.run();
            cancel.run();
            assertThat(sessionListener ? target.getSessionListenerCount() : target.getListenerCount())
                    .isEqualTo(registeredCount - 1);
        }
    }

    @Test
    public void testTableViewCachesAreReleasedBeforeAndAfterMigration() throws Exception {
        var config = MetadataStoreConfig.builder().build();
        var source = new LocalMemoryMetadataStore("memory:local", config);
        try (var dual = new DualMetadataStore(source, config)) {
            int sourceBaseline = source.getMetadataCacheCount();
            for (int i = 0; i < 3; i++) {
                var tableView = createTableView(dual);
                try {
                    assertThat(dual.getMetadataCacheCount()).isEqualTo(1);
                    assertThat(source.getMetadataCacheCount()).isEqualTo(sourceBaseline + 1);
                } finally {
                    tableView.close();
                }
                assertThat(dual.getMetadataCacheCount()).isZero();
                assertThat(source.getMetadataCacheCount()).isEqualTo(sourceBaseline);
            }
            var migrationComplete = new CountDownLatch(1);
            dual.registerCancellableSessionListener(event -> {
                if (event == SessionEvent.SessionReestablished) {
                    migrationComplete.countDown();
                }
            });
            var migratingTableView = createTableView(dual);
            try {
                completeMigration(source);
                assertThat(migrationComplete.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(source.getMetadataCacheCount()).isEqualTo(sourceBaseline);
                assertThat(((AbstractMetadataStore) dual.targetStore).getMetadataCacheCount()).isEqualTo(1);
                migratingTableView.put("key", 1).get(10, TimeUnit.SECONDS);
            } finally {
                migratingTableView.close();
            }
            var target = (AbstractMetadataStore) dual.targetStore;
            assertThat(dual.getMetadataCacheCount()).isZero();
            assertThat(target.getMetadataCacheCount()).isZero();
            for (int i = 0; i < 3; i++) {
                var tableView = createTableView(dual);
                try {
                    assertThat(target.getMetadataCacheCount()).isEqualTo(1);
                } finally {
                    tableView.close();
                }
                assertThat(dual.getMetadataCacheCount()).isZero();
                assertThat(target.getMetadataCacheCount()).isZero();
            }
        }
    }

    @Test
    public void testClosedCacheIsNotRecreatedByMigration() throws Exception {
        var config = MetadataStoreConfig.builder().build();
        var source = new LocalMemoryMetadataStore("memory:local", config);
        try (var dual = new DualMetadataStore(source, config)) {
            int sourceBaseline = source.getMetadataCacheCount();
            var cache = (DualMetadataCache<Integer>) dual.getMetadataCache(Integer.class);
            cache.close();
            cache.close();
            assertThat(source.getMetadataCacheCount()).isEqualTo(sourceBaseline);
            var migrationComplete = new CountDownLatch(1);
            dual.registerCancellableSessionListener(event -> {
                if (event == SessionEvent.SessionReestablished) {
                    migrationComplete.countDown();
                }
            });
            completeMigration(source);
            assertThat(migrationComplete.await(10, TimeUnit.SECONDS)).isTrue();
            // A completion traversal may have captured the registration before close removed it.
            cache.handleSwitchToTargetStore();
            assertThat(dual.getMetadataCacheCount()).isZero();
            assertThat(((AbstractMetadataStore) dual.targetStore).getMetadataCacheCount()).isZero();
        }
    }

    private static MetadataStoreTableViewImpl<Integer> createTableView(DualMetadataStore store) throws Exception {
        var tableView = MetadataStoreTableViewImpl.<Integer>builder()
                .name("dual-cache-lifecycle")
                .clazz(Integer.class)
                .store(store)
                .pathPrefix("/table-view")
                .listenPathValidator(path -> path.startsWith("/table-view/"))
                .conflictResolver((oldValue, newValue) -> true)
                .timeoutInMillis(10_000)
                .build();
        tableView.start();
        return tableView;
    }

    private static void completeMigration(LocalMemoryMetadataStore source) throws Exception {
        setMigrationPhase(source, MigrationPhase.COMPLETED, "memory:" + UUID.randomUUID());
    }

    private static void setMigrationPhase(LocalMemoryMetadataStore source, MigrationPhase phase, String targetUrl)
            throws Exception {
        var state = new MigrationState(phase, targetUrl);
        source.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(state), Optional.empty())
                .get(10, TimeUnit.SECONDS);
    }

    private static final class PausingDualMetadataStore extends DualMetadataStore {
        private final CountDownLatch phaseRead = new CountDownLatch(1);
        private final CountDownLatch resumeRegistration = new CountDownLatch(1);
        private volatile Thread registrationThread;

        private PausingDualMetadataStore(LocalMemoryMetadataStore source, MetadataStoreConfig config)
                throws MetadataStoreException {
            super(source, config);
        }

        @Override
        public MigrationPhase getMigrationPhase() {
            MigrationPhase phase = super.getMigrationPhase();
            if (Thread.currentThread() == registrationThread && phase == MigrationPhase.COPYING) {
                phaseRead.countDown();
                try {
                    assertThat(resumeRegistration.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(error);
                }
            }
            return phase;
        }
    }
}
