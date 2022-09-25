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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
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

    /**
     * The data provider provide these data [TopicDomain, IsPartition].
     */
    @DataProvider
    public Object[][] topicTypes(){
        return new Object[][] {
                {TopicDomain.persistent, false},
                {TopicDomain.persistent, true},
                {TopicDomain.non_persistent, false},
                {TopicDomain.non_persistent, true},
        };
    }

    @Test(timeOut = 30 * 1000, dataProvider = "topicTypes")
    public void testTopicLookup(TopicDomain topicDomain, boolean isPartition) throws Exception {
        PulsarAdmin admin0 = getAllAdmins().get(0);

        String namespace = RandomStringUtils.randomAlphabetic(5);
        admin0.namespaces().createNamespace( "public/" + namespace, 3);
        admin0.namespaces().setAutoTopicCreation("public/" + namespace,
                AutoTopicCreationOverride.builder().allowAutoTopicCreation(false).build());

        TopicName topic = TopicName.get(topicDomain.value(), "public", namespace, "t1");
        if (isPartition) {
            admin0.topics().createPartitionedTopic(topic.getPartitionedTopicName(), 3);
        } else {
            admin0.topics().createNonPartitionedTopic(topic.getPartitionedTopicName());
        }

        // To ensure all admin could get a right lookup result.
        Set<String> lookupResultSet = new HashSet<>();
        for (PulsarAdmin pulsarAdmin : getAllAdmins()) {
            try {
                if (isPartition) {
                    lookupResultSet.add(pulsarAdmin.lookups().lookupTopic(topic.getPartition(0).toString()));
                } else {
                    lookupResultSet.add(pulsarAdmin.lookups().lookupTopic(topic.getPartitionedTopicName()));
                }
            } catch (Exception e) {
                log.error(pulsarAdmin.getServiceUrl() + " - Failed to execute lookup for topic {} .", topic, e);
                Assert.fail("Failed to execute lookup by PulsarAdmin(" + pulsarAdmin.getServiceUrl() + ").");
            }
        }
        Assert.assertEquals(lookupResultSet.size(), 1);
    }

}
