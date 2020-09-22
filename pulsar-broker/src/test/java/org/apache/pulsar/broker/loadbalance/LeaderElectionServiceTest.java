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
package org.apache.pulsar.broker.loadbalance;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class LeaderElectionServiceTest {

    private LocalBookkeeperEnsemble bkEnsemble;
    private LeaderElectionService.LeaderListener listener;

    @BeforeMethod
    public void setup() throws Exception {
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        log.info("---- bk started ----");
        listener = new LeaderElectionService.LeaderListener() {
            @Override
            public void brokerIsTheLeaderNow() {
                log.info("i am a leader");
            }

            @Override
            public void brokerIsAFollowerNow() {
                log.info("i am a follower");
            }
        };
    }

    @AfterMethod
    void shutdown() throws Exception {
        bkEnsemble.stop();
        log.info("---- bk stopped ----");
    }


    @Test
    public void electedShouldBeTrue() {
        final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        final String safeWebServiceAddress = "http://localhost:8080";
        ZooKeeperCache zkCache = Mockito.mock(ZooKeeperCache.class);
        PulsarService pulsar = Mockito.mock(PulsarService.class);

        Mockito.when(pulsar.getZkClient()).thenReturn(bkEnsemble.getZkClient());
        Mockito.when(pulsar.getExecutor()).thenReturn(ses);
        Mockito.when(pulsar.getSafeWebServiceAddress()).thenReturn(safeWebServiceAddress);

        Mockito.when(zkCache.getZooKeeper()).thenReturn(bkEnsemble.getZkClient());
        Mockito.when(pulsar.getLocalZkCache()).thenReturn(zkCache);

        LeaderElectionService leaderElectionService = new LeaderElectionService(pulsar, listener);
        leaderElectionService.start();
        Assert.assertTrue(leaderElectionService.isElected());
        Assert.assertTrue(leaderElectionService.isLeader());
        Assert.assertEquals(leaderElectionService.getCurrentLeader().getServiceUrl(), safeWebServiceAddress);
        log.info("leader state {} {} {}",
                leaderElectionService.isElected(),
                leaderElectionService.isLeader(),
                leaderElectionService.getCurrentLeader().getServiceUrl());

        LeaderElectionService followerElectionService = new LeaderElectionService(pulsar, listener);
        followerElectionService.start();
        Assert.assertTrue(followerElectionService.isElected());
        Assert.assertFalse(followerElectionService.isLeader());
        Assert.assertEquals(followerElectionService.getCurrentLeader().getServiceUrl(), safeWebServiceAddress);
        log.info("follower state {} {} {}",
                followerElectionService.isElected(),
                followerElectionService.isLeader(),
                followerElectionService.getCurrentLeader().getServiceUrl());
    }

    @Test
    public void anErrorShouldBeThrowBeforeLeaderElected() throws PulsarServerException, PulsarClientException, PulsarAdminException {
        final String clusterName = "elect-test";
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setWebServicePort(Optional.of(8080));
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        PulsarService pulsar = Mockito.spy(new MockPulsarService(config));
        pulsar.start();

        // broker and webService is started, but leaderElectionService not ready
        Mockito.doReturn(null).when(pulsar).getLeaderElectionService();
        final String tenant = "elect";
        final String namespace = "ns";
        PulsarAdmin adminClient = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build();
        adminClient.clusters().createCluster(clusterName, new ClusterData("http://localhost:8080"));
        adminClient.tenants().createTenant(tenant, new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(clusterName)));
        adminClient.namespaces().createNamespace(tenant + "/" + namespace, 16);
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .startingBackoffInterval(1, TimeUnit.MILLISECONDS)
                .maxBackoffInterval(100, TimeUnit.MILLISECONDS)
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        checkLookupException(tenant, namespace, client);

        // broker, webService and leaderElectionService is started, but elect not ready;
        LeaderElectionService leaderElectionService = Mockito.mock(LeaderElectionService.class);
        Mockito.when(leaderElectionService.isElected()).thenReturn(false);
        Mockito.doReturn(leaderElectionService).when(pulsar).getLeaderElectionService();
        checkLookupException(tenant, namespace, client);

        // broker, webService and leaderElectionService is started, and elect is done;
        Mockito.when(leaderElectionService.isElected()).thenReturn(true);
        Mockito.when(leaderElectionService.isLeader()).thenReturn(true);
        Mockito.when(leaderElectionService.getCurrentLeader()).thenReturn(new LeaderBroker("http://localhost:8080"));

        Producer<byte[]> producer = client.newProducer()
                .topic("persistent://" + tenant + "/" + namespace + "/1p")
                .create();
        producer.getTopic();

    }

    private void checkLookupException(String tenant, String namespace, PulsarClient client) {
        try {
            client.newProducer()
                    .topic("persistent://" + tenant + "/" + namespace + "/1p")
                    .create();
        } catch (PulsarClientException t) {
            Assert.assertTrue(t instanceof PulsarClientException.LookupException);
            Assert.assertEquals(t.getMessage(), "java.lang.IllegalStateException: The leader election has not yet been completed!");
        }
    }


    private static class MockPulsarService extends PulsarService {

        public MockPulsarService(ServiceConfiguration config) {
            super(config);
        }

        public MockPulsarService(ServiceConfiguration config, Optional<WorkerService> functionWorkerService, Consumer<Integer> processTerminator) {
            super(config, functionWorkerService, processTerminator);
        }

        @Override
        protected void startLeaderElectionService() {
            // mock
        }
    }

}
