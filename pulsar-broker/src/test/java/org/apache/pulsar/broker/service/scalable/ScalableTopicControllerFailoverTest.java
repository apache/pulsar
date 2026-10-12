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
package org.apache.pulsar.broker.service.scalable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.ScalableTopics;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.DualMetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Controller-lock failover between brokers: the {@link ScalableTopicController} must follow its leader
 * election when the election changes state by itself, after the initial election. Runs on a real
 * ZooKeeper server so that every broker has its own metadata session, as the controller lock is an
 * ephemeral node owned by the leader's session.
 */
public class ScalableTopicControllerFailoverTest {

    private static final int SEGMENTS = 2;

    private TestZKServer zks;
    private TopicName topic;
    private Broker broker1;
    private Broker broker2;

    @BeforeClass(alwaysRun = true)
    public void startZooKeeper() throws Exception {
        zks = new TestZKServer();
    }

    @AfterClass(alwaysRun = true)
    public void stopZooKeeper() throws Exception {
        if (zks != null) {
            zks.close();
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        topic = TopicName.get("topic://tenant/ns/topic-" + UUID.randomUUID().toString().substring(0, 8));
        broker1 = new Broker("broker-1");
        broker2 = new Broker("broker-2");
        broker1.resources.createScalableTopicAsync(topic,
                ScalableTopicController.createInitialMetadata(SEGMENTS, 4, Map.of())).get();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (broker1 != null) {
            broker1.close();
        }
        if (broker2 != null) {
            broker2.close();
        }
    }

    /**
     * The leader goes away, as when its broker shuts down or crashes, and the controller lock is released:
     * the election of the follower takes the lock over by itself. The follower must then serve as the
     * leader, from the layout and the consumer sessions the previous leader left behind.
     */
    @Test
    public void testFollowerServesAsLeaderAfterTakingTheLockOver() throws Exception {
        ScalableTopicController leader = broker1.newController();
        leader.initialize().get();
        leader.registerConsumer("sub", "consumer-1", 1L, ScalableConsumerType.STREAM,
                mock(TransportCnx.class)).get();
        ScalableTopicController follower = broker2.newController();
        follower.initialize().get();
        assertThat(follower.isLeader()).as("broker 2, following broker 1").isFalse();
        // The layout changes after the follower loaded it.
        leader.splitSegment(1).get();

        leader.close().get();

        Awaitility.await().untilAsserted(() -> assertThat(follower.isLeader())
                .as("broker 2, whose election took the controller lock over").isTrue());
        assertThat(follower.getLayout().get().getAllSegments())
                .as("segments of the layout the new leader serves")
                .hasSize(SEGMENTS + 2);
        // consumer-1 is restored from its persisted session: it keeps one of the two segments it can be
        // assigned until the sealed segment drains, and consumer-2 gets the other one.
        ConsumerAssignment assignment = follower.registerConsumer("sub", "consumer-2", 2L,
                ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();
        assertThat(assignment.assignedSegments())
                .as("segments assigned to consumer-2 next to the restored consumer-1")
                .hasSize(1);
    }

    /**
     * The leader loses the controller lock to another broker without releasing it, as when its metadata
     * session expires and the other broker takes the lock over before it reconnects: its election then
     * follows the new leader, and it must stop serving as the leader.
     */
    @Test
    public void testLeaderStopsServingWhenAnotherBrokerTakesTheLockOver() throws Exception {
        ScalableTopicController first = broker1.newController();
        first.initialize().get();
        ScalableTopicController second = broker2.newController();
        second.initialize().get();
        assertThat(first.isLeader()).as("broker 1, elected first").isTrue();

        broker2.takeOverControllerLock();

        Awaitility.await().untilAsserted(() -> {
            assertThat(second.isLeader()).as("broker 2, now holding the controller lock").isTrue();
            assertThat(first.isLeader()).as("broker 1, which lost the controller lock").isFalse();
        });
        assertThatThrownBy(() -> first.registerConsumer("sub", "consumer-1", 1L,
                ScalableConsumerType.STREAM, mock(TransportCnx.class)))
                .hasMessageContaining("not the leader");
    }

    /**
     * A controller created again for a topic on a broker that released an earlier one, as when the topic
     * is deleted and created again with the same name, must follow its own leader election: the coordination
     * service keeps one election per lock path, and the released controller's would report to it.
     */
    @Test
    public void testRecreatedControllerServesAsLeaderAfterTakingTheLockOver() throws Exception {
        ScalableTopicController released = broker1.newController();
        released.initialize().get();
        released.close().get();
        ScalableTopicController leader = broker2.newController();
        leader.initialize().get();
        ScalableTopicController recreated = broker1.newController();
        recreated.initialize().get();
        assertThat(recreated.isLeader()).as("broker 1, following broker 2").isFalse();

        leader.close().get();

        Awaitility.await().untilAsserted(() -> assertThat(recreated.isLeader())
                .as("broker 1, whose election took the controller lock over").isTrue());
    }

    /** A broker of the test: its own metadata session, coordination service and controllers. */
    private final class Broker implements AutoCloseable {
        private final String brokerId;
        private final MetadataStoreExtended store;
        private final CoordinationService coordinationService;
        private final ScalableTopicResources resources;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private final BrokerService brokerService = mock(BrokerService.class);
        private final List<ScalableTopicController> controllers = new ArrayList<>();

        Broker(String brokerId) throws Exception {
            this.brokerId = brokerId;
            store = MetadataStoreExtended.create(zks.getConnectionString(), MetadataStoreConfig.builder().build());
            coordinationService = new CoordinationServiceImpl(store);
            resources = new ScalableTopicResources(store, 30);

            PulsarService pulsar = mock(PulsarService.class);
            PulsarAdmin admin = mock(PulsarAdmin.class);
            ScalableTopics scalableTopics = mock(ScalableTopics.class);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(pulsar.getBrokerId()).thenReturn(brokerId);
            when(pulsar.getExecutor()).thenReturn(scheduler);
            when(pulsar.getAdminClient()).thenReturn(admin);
            when(admin.scalableTopics()).thenReturn(scalableTopics);
            when(scalableTopics.createSegmentAsync(anyString(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(scalableTopics.terminateSegmentAsync(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            // No segment ever drains.
            when(scalableTopics.getSegmentSubscriptionBacklogAsync(anyString(), anyString()))
                    .thenReturn(CompletableFuture.completedFuture(1L));
        }

        ScalableTopicController newController() {
            ScalableTopicController controller =
                    new ScalableTopicController(topic, resources, brokerService, coordinationService);
            controllers.add(controller);
            return controller;
        }

        /**
         * Replace the controller lock with one owned by this broker's session, in a single transaction: the
         * other brokers' elections, re-electing when they see the lock deleted, find this broker holding it.
         */
        void takeOverControllerLock() throws Exception {
            String lockPath = resources.controllerLockPath(topic);
            byte[] value = ObjectMapperFactory.getMapper().writer().writeValueAsBytes(brokerId);
            ((ZKMetadataStore) ((DualMetadataStore) store).getSourceStore()).getZkClient().multi(List.of(
                    Op.delete(lockPath, -1),
                    Op.create(lockPath, value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)));
        }

        @Override
        public void close() throws Exception {
            for (ScalableTopicController controller : controllers) {
                controller.close().get();
            }
            coordinationService.close();
            store.close();
            scheduler.shutdownNow();
        }
    }
}
