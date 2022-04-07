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
package org.apache.pulsar.broker.admin.v3;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class AdminApiTransactionMultiBrokerTest extends TransactionTestBase {

    private static final int NUM_BROKERS = 16;
    private static final int NUM_PARTITIONS = 3;

    @BeforeMethod
    protected void setup() throws Exception {
        setUpBase(NUM_BROKERS, NUM_PARTITIONS, NAMESPACE1 + "/test", 0);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testRedirectOfGetCoordinatorInternalStats() throws Exception {
        Map<String, String> map = admin.lookups()
                .lookupPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
        while (map.containsValue(getPulsarServiceList().get(0).getBrokerServiceUrl())) {
            admin.topics().deletePartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
            admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), NUM_PARTITIONS);
            map = admin.lookups().lookupPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
        }
        //init tc stores
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            admin.transactions().getCoordinatorInternalStats(i, false);
        }
    }
}
