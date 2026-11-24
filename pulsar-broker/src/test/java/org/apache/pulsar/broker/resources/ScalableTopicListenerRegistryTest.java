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
package org.apache.pulsar.broker.resources;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Coverage for the {@code MetadataPathListener} registry on
 * {@link ScalableTopicResources}: register / deregister behaviour, exact-path
 * dispatch, and the no-leak property the registry exists to enforce.
 *
 * <p>The bug it prevents: pre-registry, every {@code DagWatchSession} called
 * {@code MetadataStore.registerListener} directly. The store has no
 * {@code unregisterListener}, so each closed session left a stale listener
 * registered for the broker's lifetime — both a memory leak and a
 * per-notification dispatch tax that grew linearly with total sessions ever
 * opened.
 */
public class ScalableTopicListenerRegistryTest {

    private MetadataStoreExtended store;
    private ScalableTopicResources resources;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().build());
        resources = new ScalableTopicResources(store, 30);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void deregisterStopsDispatchToListener() {
        // The whole point of the registry: a deregistered listener must NOT receive
        // events even if the path still matches. This is the leak fix proper —
        // pre-registry, a closed session would keep ticking on every namespace event
        // for the broker's lifetime.
        var listener = new RecordingListener("/topics/tenant/ns/some-topic");
        resources.registerPathListener(listener);
        resources.handleNotification(new Notification(NotificationType.Modified,
                "/topics/tenant/ns/some-topic"));
        assertEquals(listener.received.size(), 1, "registered listener should see its event");

        resources.deregisterPathListener(listener);
        resources.handleNotification(new Notification(NotificationType.Modified,
                "/topics/tenant/ns/some-topic"));
        assertEquals(listener.received.size(), 1,
                "deregistered listener must not see further events");
    }

    @Test
    public void dispatchesOnlyOnExactPathMatch() {
        // Path filter is exact-equal — a notification for a sibling topic must not
        // wake up listeners on a different topic.
        var watching = new RecordingListener("/topics/tenant/ns/a");
        var bystander = new RecordingListener("/topics/tenant/ns/b");
        resources.registerPathListener(watching);
        resources.registerPathListener(bystander);

        resources.handleNotification(new Notification(NotificationType.Modified,
                "/topics/tenant/ns/a"));

        assertEquals(watching.received.size(), 1);
        assertTrue(bystander.received.isEmpty(),
                "sibling topic listener must not receive events on /a");
    }

    @Test
    public void multipleListenersOnSamePathAllReceive() {
        // Two sessions can watch the same topic (rare but legal) — both must fire.
        String path = "/topics/tenant/ns/shared";
        var l1 = new RecordingListener(path);
        var l2 = new RecordingListener(path);
        resources.registerPathListener(l1);
        resources.registerPathListener(l2);

        resources.handleNotification(new Notification(NotificationType.Modified, path));

        assertEquals(l1.received.size(), 1);
        assertEquals(l2.received.size(), 1);
    }

    @Test
    public void listenerExceptionDoesNotInterruptOtherListeners() {
        // One bad listener throwing must not poison the dispatch loop — the next
        // listener on the same path must still see the event.
        String path = "/topics/tenant/ns/x";
        var throwing = new ScalableTopicResources.MetadataPathListener() {
            @Override public String getMetadataPath() {
                return path;
            }
            @Override public void onNotification(Notification notification) {
                throw new RuntimeException("boom");
            }
        };
        var ok = new RecordingListener(path);
        resources.registerPathListener(throwing);
        resources.registerPathListener(ok);

        resources.handleNotification(new Notification(NotificationType.Modified, path));

        assertEquals(ok.received.size(), 1, "well-behaved listener should still see the event");
    }

    @Test
    public void deregisterIsIdempotent() {
        var listener = new RecordingListener("/topics/tenant/ns/a");
        resources.registerPathListener(listener);
        resources.deregisterPathListener(listener);
        resources.deregisterPathListener(listener); // must not throw
        // Deregistering one we never registered must also be silent.
        resources.deregisterPathListener(new RecordingListener("/topics/tenant/ns/never"));
    }

    private static final class RecordingListener
            implements ScalableTopicResources.MetadataPathListener {
        final String path;
        final List<Notification> received = new ArrayList<>();

        RecordingListener(String path) {
            this.path = path;
        }

        @Override
        public String getMetadataPath() {
            return path;
        }

        @Override
        public void onNotification(Notification notification) {
            received.add(notification);
        }
    }
}
