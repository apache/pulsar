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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class MultiBrokerLeaderElectionTest extends MultiBrokerBaseTest {
    @Override
    protected int numberOfAdditionalBrokers() {
        return 9;
    }

    TestZKServer testZKServer;

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        testZKServer = new TestZKServer();
    }

    @Override
    protected void onCleanup() {
        super.onCleanup();
        if (testZKServer != null) {
            try {
                testZKServer.close();
            } catch (Exception e) {
                log.error("Error in stopping ZK server", e);
            }
        }
    }

    @Override
    protected MetadataStoreExtended createLocalMetadataStore() throws MetadataStoreException {
        return MetadataStoreExtended.create(testZKServer.getConnectionString(), MetadataStoreConfig.builder().build());
    }

    @Override
    protected MetadataStoreExtended createConfigurationMetadataStore() throws MetadataStoreException {
        return MetadataStoreExtended.create(testZKServer.getConnectionString(), MetadataStoreConfig.builder().build());
    }

    @Test
    public void shouldElectOneLeader() {
        int leaders = 0;
        for (PulsarService broker : getAllBrokers()) {
            if (broker.getLeaderElectionService().isLeader()) {
                leaders++;
            }
        }
        assertEquals(leaders, 1);
    }

    @Test
    public void shouldAllBrokersKnowTheLeader() {
        Awaitility.await().untilAsserted(() -> {
            for (PulsarService broker : getAllBrokers()) {
                Optional<LeaderBroker> currentLeader = broker.getLeaderElectionService().getCurrentLeader();
                assertTrue(currentLeader.isPresent(), "Leader wasn't known on broker " + broker.getBrokerServiceUrl());
            }
        });
    }

    @Test
    public void shouldAllBrokersBeAbleToGetTheLeader() {
        Awaitility.await().untilAsserted(() -> {
            LeaderBroker leader = null;
            for (PulsarService broker : getAllBrokers()) {
                Optional<LeaderBroker> currentLeader =
                        broker.getLeaderElectionService().readCurrentLeader().get(1, TimeUnit.SECONDS);
                assertTrue(currentLeader.isPresent(), "Leader wasn't known on broker " + broker.getBrokerServiceUrl());
                if (leader != null) {
                    assertEquals(currentLeader.get(), leader,
                            "Different leader on broker " + broker.getBrokerServiceUrl());
                } else {
                    leader = currentLeader.get();
                }
            }
        });
    }

    @Test
    public void shouldProvideConsistentAnswerToTopicLookups()
            throws PulsarAdminException, ExecutionException, InterruptedException {
        String topicNameBase = "persistent://public/default/lookuptest" + UUID.randomUUID() + "-";
        List<String> topicNames = IntStream.range(0, 500).mapToObj(i -> topicNameBase + i)
                .collect(Collectors.toList());
        List<PulsarAdmin> allAdmins = getAllAdmins();
        @Cleanup("shutdown")
        ExecutorService executorService = Executors.newFixedThreadPool(allAdmins.size());
        List<Future<List<String>>> resultFutures = new ArrayList<>();
        String leaderBrokerUrl = admin.brokers().getLeaderBroker().getServiceUrl();
        log.info("LEADER is {}", leaderBrokerUrl);
        // use Phaser to increase the chances of a race condition by triggering all threads once
        // they are waiting just before the lookupTopic call
        final Phaser phaser = new Phaser(1);
        for (PulsarAdmin brokerAdmin : allAdmins) {
            if (!leaderBrokerUrl.equals(brokerAdmin.getServiceUrl())) {
                phaser.register();
                log.info("Doing lookup to broker {}", brokerAdmin.getServiceUrl());
                resultFutures.add(executorService.submit(() -> {
                    phaser.arriveAndAwaitAdvance();
                    return topicNames.stream().map(topicName -> {
                        try {
                            return brokerAdmin.lookups().lookupTopic(topicName);
                        } catch (PulsarAdminException e) {
                            log.error("Error looking up topic {} in {}", topicName, brokerAdmin.getServiceUrl());
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList());
                }));
            }
        }
        phaser.arriveAndAwaitAdvance();
        List<String> firstResult = null;
        for (Future<List<String>> resultFuture : resultFutures) {
            List<String> result = resultFuture.get();
            if (firstResult == null) {
                firstResult = result;
            } else {
                assertEquals(result, firstResult, "The lookup results weren't consistent.");
            }
        }
    }
}
