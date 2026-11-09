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
package org.apache.pulsar.metadata.impl.oxia;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Runs the session watcher against a real Oxia server: the canary lifecycle, the notification
 * round-trip and the re-establishment of the session are exercised end to end. The loss is
 * triggered by deleting the canary, which is deterministic and delivers exactly the signal a
 * server-side session sweep delivers — the watcher's only source of truth is the canary's
 * {@code KeyDeleted} on the notification stream.
 */
public class OxiaSessionWatcherIntegrationTest {

    private OxiaContainer oxiaServer;

    private synchronized String getOxiaServerConnectString() {
        if (oxiaServer == null) {
            oxiaServer = new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME);
            oxiaServer.start();
        }
        return oxiaServer.getServiceAddress();
    }

    @AfterClass(alwaysRun = true)
    void stopOxiaServer() {
        if (oxiaServer != null) {
            oxiaServer.stop();
        }
    }

    @Test
    public void sessionLossIsObservedAndRecoveredAgainstARealServer() throws Exception {
        String address = getOxiaServerConnectString();

        try (MetadataStoreExtended store = MetadataStoreExtended.create(
                "oxia://" + address, MetadataStoreConfig.builder().build());
                AsyncOxiaClient observer = OxiaClientBuilder.create(address)
                        .clientIdentifier("session-watcher-integration-test")
                        .namespace("default")
                        .asyncClient()
                        .get()) {
            List<SessionEvent> sessionEvents = new CopyOnWriteArrayList<>();
            List<org.apache.pulsar.metadata.api.Notification> notifications =
                    new CopyOnWriteArrayList<>();
            store.registerSessionListener(sessionEvents::add);
            store.registerListener(notifications::add);

            // The canary of the current incarnation, created on the store's session.
            List<String> canaries = awaitCanaries(observer);
            assertThat(canaries).hasSize(1);
            String canary = canaries.get(0);

            assertTrue(observer.delete(canary, Set.of()).join(),
                    "deleting the canary must succeed");

            await().atMost(30, SECONDS).untilAsserted(() -> assertThat(sessionEvents).containsExactly(
                    SessionEvent.SessionLost, SessionEvent.SessionReestablished));

            // The reserved namespace never surfaces as ordinary notifications, and after the
            // recovery exactly one canary exists again: the fresh incarnation.
            assertThat(notifications).isEmpty();
            assertThat(awaitCanaries(observer)).hasSize(1);
        }
    }

    /**
     * Waits until the reserved namespace holds at least one canary — the store writes it on its
     * session once the watcher starts — and returns the keys it holds.
     */
    private static List<String> awaitCanaries(AsyncOxiaClient observer) {
        return await().atMost(30, SECONDS).until(
                () -> observer.list(OxiaSessionWatcher.CANARY_KEY_PREFIX, "/", Set.of()).join(),
                canaries -> !canaries.isEmpty());
    }
}
