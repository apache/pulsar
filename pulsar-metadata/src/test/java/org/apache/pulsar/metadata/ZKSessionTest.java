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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "quarantine")
public class ZKSessionTest extends BaseMetadataStoreTest {

    @Test
    public void testDisconnection() throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis(300_000)
                        .build());

        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);

        zks.stop();

        SessionEvent e = sessionEvents.poll(5, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.ConnectionLost);

        zks.start();
        e = sessionEvents.poll(20, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.Reconnected);

        e = sessionEvents.poll(5, TimeUnit.SECONDS);
        assertNull(e);
    }

    @Test
    public void testSessionLost() throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis(5_000)
                        .build());

        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);

        zks.stop();

        SessionEvent e = sessionEvents.poll(5, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.ConnectionLost);

        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.SessionLost);

        zks.start();
        boolean zkServerReady = zks.waitForServerUp(zks.getConnectionString(), 30_000);
        assertTrue(zkServerReady);
        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.Reconnected);
        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.SessionReestablished);

        e = sessionEvents.poll(1, TimeUnit.SECONDS);
        assertNull(e);
    }

    @Test
    public void testReacquireLocksAfterSessionLost() throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis(2_000)
                        .build());

        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);
        @Cleanup
        LockManager<String> lm1 = coordinationService.getLockManager(String.class);

        String path = newKey();

        ResourceLock<String> lock = lm1.acquireLock(path, "value-1").join();


        zks.expireSession(((ZKMetadataStore) store).getZkSessionId());

        SessionEvent e = sessionEvents.poll(5, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.ConnectionLost);

        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.SessionLost);

        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.Reconnected);
        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.SessionReestablished);

        Awaitility.await().untilAsserted(() -> assertTrue(store.get(path).join().isPresent()));
        assertFalse(lock.getLockExpiredFuture().isDone());
    }

    @Test
    public void testReacquireLeadershipAfterSessionLost() throws Exception {
        //  ---  init
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis(2_000)
                        .build());

        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);
        BlockingQueue<LeaderElectionState> leaderElectionEvents = new LinkedBlockingQueue<>();
        String path = newKey();

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);
        @Cleanup
        LeaderElection<String> le1 = coordinationService.getLeaderElection(String.class, path,
                leaderElectionEvents::add);
        // --- test manual elect
        le1.elect("value-1").join();
        assertEquals(le1.getState(), LeaderElectionState.Leading);

        LeaderElectionState les = leaderElectionEvents.poll(5, TimeUnit.SECONDS);
        assertEquals(les, LeaderElectionState.Leading);
        // --- expire session
        zks.expireSession(((ZKMetadataStore) store).getZkSessionId());

        SessionEvent e = sessionEvents.poll(5, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.ConnectionLost);

        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.SessionLost);
        // --- test  le1 can be leader
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .untilAsserted(()-> assertEquals(le1.getState(),LeaderElectionState.Leading)); // reacquire leadership
        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.Reconnected);
        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.SessionReestablished);
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .untilAsserted(()-> assertEquals(le1.getState(),LeaderElectionState.Leading));
        assertTrue(store.get(path).join().isPresent());
    }
}
