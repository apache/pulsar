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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LeaderElectionServiceTest {

    private LocalBookkeeperEnsemble bkEnsemble;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        log.info("---- bk started ----");
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        bkEnsemble.stop();
        log.info("---- bk stopped ----");
    }

    @Test
    public void anErrorShouldBeThrowBeforeLeaderElected() throws PulsarServerException, PulsarClientException,
            PulsarAdminException {
        final String clusterName = "elect-test";
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        @Cleanup
        PulsarService pulsar = Mockito.spy(new MockPulsarService(config));
        pulsar.start();

        // mock pulsar.getLeaderElectionService() in a thread safe way
        AtomicReference<LeaderElectionService> leaderElectionServiceReference = new AtomicReference<>();
        Mockito.doAnswer(invocation -> leaderElectionServiceReference.get())
                .when(pulsar).getLeaderElectionService();

        // broker and webService is started, but leaderElectionService not ready
        final String tenant = "elect";
        final String namespace = "ns";
        @Cleanup
        PulsarAdmin adminClient = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build();
        adminClient.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        adminClient.tenants().createTenant(tenant, new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"),
                Sets.newHashSet(clusterName)));
        adminClient.namespaces().createNamespace(tenant + "/" + namespace, 16);
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .startingBackoffInterval(1, TimeUnit.MILLISECONDS)
                .maxBackoffInterval(100, TimeUnit.MILLISECONDS)
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        checkLookupException(tenant, namespace, client);

        // setup LeaderElectionService mock in a thread safe way
        LeaderElectionService leaderElectionService = Mockito.mock(LeaderElectionService.class);
        AtomicReference<LeaderBroker> leaderBrokerReference = new AtomicReference<>();
        Mockito.when(leaderElectionService.isLeader()).thenAnswer(invocation ->
                leaderBrokerReference.get() != null);
        Mockito.when(leaderElectionService.getCurrentLeader())
                .thenAnswer(invocation -> Optional.ofNullable(leaderBrokerReference.get()));
        leaderElectionServiceReference.set(leaderElectionService);

        // broker, webService and leaderElectionService is started, but elect not ready;
        checkLookupException(tenant, namespace, client);

        // broker, webService and leaderElectionService is started, and elect is done;
        leaderBrokerReference.set(new LeaderBroker(pulsar.getWebServiceAddress()));

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
            Assert.assertTrue(
                    t.getMessage().contains(
                            "java.lang.IllegalStateException: The leader election has not yet been completed!"));
        }
    }

    private static class MockPulsarService extends PulsarService {

        public MockPulsarService(ServiceConfiguration config) {
            super(config);
        }

        public MockPulsarService(ServiceConfiguration config,
                                 Optional<WorkerService> functionWorkerService,
                                 Consumer<Integer> processTerminator) {
            super(config, functionWorkerService, processTerminator);
        }

        @Override
        protected void startLeaderElectionService() {
            // mock
        }
    }

}
