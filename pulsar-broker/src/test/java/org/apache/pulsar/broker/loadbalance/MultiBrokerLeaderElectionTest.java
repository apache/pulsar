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
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class MultiBrokerLeaderElectionTest extends MultiBrokerBaseTest {
    @Override
    protected int numberOfAdditionalBrokers() {
        return 9;
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
    public void shouldProvideConsistentAnswerToTopicLookup()
            throws PulsarAdminException, PulsarClientException, ExecutionException, InterruptedException {
        String topicName = "persistent://public/default/lookuptest" + UUID.randomUUID();

        List<PulsarAdmin> allAdmins = getAllAdmins();
        @Cleanup("shutdown")
        ExecutorService executorService = Executors.newFixedThreadPool(allAdmins.size());
        List<Future<String>> resultFutures = new ArrayList<>();
        String leaderBrokerUrl = admin.brokers().getLeaderBroker().getServiceUrl();
        log.info("LEADER is {}", leaderBrokerUrl);
        // wait 2 seconds to increase the likelyhood of the race condition
        Thread.sleep(2000L);
        for (PulsarAdmin brokerAdmin : allAdmins) {
            if (!leaderBrokerUrl.equals(brokerAdmin.getServiceUrl())) {
                log.info("Doing lookup to broker {}", brokerAdmin.getServiceUrl());
                resultFutures.add(executorService.submit(() -> brokerAdmin.lookups().lookupTopic(topicName)));
            }
        }
        String firstResult = null;
        for (Future<String> resultFuture : resultFutures) {
            String result = resultFuture.get();
            log.info("LOOKUP RESULT {}", result);
            if (firstResult == null) {
                firstResult = result;
            } else {
                assertEquals(result, firstResult, "The lookup results weren't consistent.");
            }
        }
    }
}
