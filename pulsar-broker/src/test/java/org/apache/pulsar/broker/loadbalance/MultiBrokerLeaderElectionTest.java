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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MultiBrokerLeaderElectionTest extends MultiBrokerBaseTest {

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
}
