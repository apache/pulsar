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
package org.apache.pulsar.broker.admin;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.Test;

/**
 * Test multi-broker admin api.
 */
@Slf4j
@Test(groups = "broker-admin")
public class AdminApiMultiBrokersTest extends MultiBrokerBaseTest {
    @Override
    protected int numberOfAdditionalBrokers() {
        return 3;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
    }

    @Override
    protected void onCleanup() {
        super.onCleanup();
    }

    @Test(timeOut = 30 * 1000)
    public void testGetLeaderBroker()
            throws ExecutionException, InterruptedException, PulsarAdminException {
        List<PulsarService> allBrokers = getAllBrokers();
        Optional<LeaderBroker> leaderBroker =
                allBrokers.get(0).getLeaderElectionService().readCurrentLeader().get();
        assertTrue(leaderBroker.isPresent());
        log.info("Leader broker is {}", leaderBroker);
        for (PulsarAdmin admin : getAllAdmins()) {
            String serviceUrl = admin.brokers().getLeaderBroker().getServiceUrl();
            log.info("Pulsar admin get leader broker is {}", serviceUrl);
            assertEquals(leaderBroker.get().getServiceUrl(), serviceUrl);
        }
    }

}
