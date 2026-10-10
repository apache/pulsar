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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.MockZooKeeperSession;
import org.testng.annotations.Test;

/**
 * Tests for closing a leader election instance whose view of the leader node is stale: a metadata
 * store session expiry removes the ephemeral leader node server-side, and another participant can
 * create its own leader node before the former leader observes the Deleted notification. Closing
 * such a stale leader must never delete the node created by the other session.
 */
public class LeaderElectionStaleLeaderCloseTest {

    private static final String PATH = "/test/stale-leader-close/leader";

    @Test(timeOut = 30000)
    public void closeOfActualLeaderStillDeletesItsOwnNode() throws Exception {
        @Cleanup
        MockZooKeeper mockZooKeeper = MockZooKeeper.newInstance();
        @Cleanup
        MockZooKeeperSession session1 = MockZooKeeperSession.newInstance(mockZooKeeper, false);
        @Cleanup
        ZKMetadataStore store1 = new ZKMetadataStore(session1, MetadataStoreConfig.builder().build(), true);

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(store1);
        @Cleanup
        LeaderElection<String> le1 = cs1.getLeaderElection(String.class, PATH, __ -> { });

        assertEquals(le1.elect("test-1").join(), LeaderElectionState.Leading);
        assertTrue(store1.get(PATH).join().orElseThrow().getStat().isCreatedBySelf());

        le1.close();

        assertEquals(store1.get(PATH).join(), Optional.empty());
        assertEquals(le1.getState(), LeaderElectionState.NoLeader);
    }

    @Test(timeOut = 30000)
    public void closeOfStaleLeaderAfterFollowerTakeoverDoesNotDeleteTheNewLeadersNode() throws Exception {
        @Cleanup
        MockZooKeeper mockZooKeeper = MockZooKeeper.newInstance();
        @Cleanup
        MockZooKeeperSession session1 = MockZooKeeperSession.newInstance(mockZooKeeper, false);
        @Cleanup
        MockZooKeeperSession session2 = MockZooKeeperSession.newInstance(mockZooKeeper, false);
        @Cleanup
        ZKMetadataStore store1 = new ZKMetadataStore(session1, MetadataStoreConfig.builder().build(), true);
        @Cleanup
        ZKMetadataStore store2 = new ZKMetadataStore(session2, MetadataStoreConfig.builder().build(), true);

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(store1);
        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store2);

        List<LeaderElectionState> le1States = new CopyOnWriteArrayList<>();
        List<LeaderElectionState> le2States = new CopyOnWriteArrayList<>();
        CountDownLatch le2Leading = new CountDownLatch(1);

        @Cleanup
        LeaderElection<String> le1 = cs1.getLeaderElection(String.class, PATH, le1States::add);
        @Cleanup
        LeaderElection<String> le2 = cs2.getLeaderElection(String.class, PATH, state -> {
            le2States.add(state);
            if (state == LeaderElectionState.Leading) {
                le2Leading.countDown();
            }
        });

        assertEquals(le1.elect("test-1").join(), LeaderElectionState.Leading);
        assertEquals(le2.elect("test-2").join(), LeaderElectionState.Following);

        // session1 expires: the server drops the dead session's watches, removes its ephemeral node
        // and delivers the NodeDeleted watch to the remaining live sessions. Only le2's session is
        // still alive, so only le2 learns about the deletion.
        mockZooKeeper.deleteWatchers(session1.getSessionId());
        mockZooKeeper.delete(PATH, -1);

        // le2 takes over through its own machinery: Deleted notification -> re-election -> creating
        // its own ephemeral leader node.
        assertTrue(le2Leading.await(20, TimeUnit.SECONDS));
        assertEquals(le2.getState(), LeaderElectionState.Leading);
        assertEquals(le2.getLeaderValueIfPresent(), Optional.of("test-2"));

        // le1 saw nothing and still believes it is the leader.
        assertEquals(le1States, List.of(LeaderElectionState.Leading));
        assertEquals(le1.getState(), LeaderElectionState.Leading);
        GetResult foreignNode = store1.get(PATH).join().orElse(null);
        assertNotNull(foreignNode);
        assertFalse(foreignNode.getStat().isCreatedBySelf(),
                "precondition: from le1's session the node must be foreign");

        le1.close();

        GetResult survivor = store1.get(PATH).join().orElse(null);
        assertNotNull(survivor, "the stale leader's close deleted the node created by the new leader");
        assertTrue(store2.get(PATH).join().orElseThrow().getStat().isCreatedBySelf(),
                "the surviving node must be the one le2's session created");
        assertEquals(le1.getState(), LeaderElectionState.NoLeader);
        // le2 kept the leadership: exactly the initial Following and the takeover Leading, with no
        // extra loss/regain cycle triggered by le1's close.
        assertEquals(le2States, List.of(LeaderElectionState.Following, LeaderElectionState.Leading));
        assertEquals(le2.getState(), LeaderElectionState.Leading);
    }

}
