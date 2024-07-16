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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
public class LookupServiceTest extends ProducerConsumerBase {

    private PulsarClientImpl clientWithHttpLookup;
    private PulsarClientImpl clientWitBinaryLookup;

    private boolean enableBrokerSideSubscriptionPatternEvaluation = true;
    private int subscriptionPatternMaxLength = 10_000;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        clientWithHttpLookup =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        clientWitBinaryLookup =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (clientWithHttpLookup != null) {
            clientWithHttpLookup.close();
        }
        if (clientWitBinaryLookup != null) {
            clientWitBinaryLookup.close();
        }
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setEnableBrokerSideSubscriptionPatternEvaluation(enableBrokerSideSubscriptionPatternEvaluation);
        conf.setSubscriptionPatternMaxLength(subscriptionPatternMaxLength);
    }

    private LookupService getLookupService(boolean isUsingHttpLookup) {
        if (isUsingHttpLookup) {
            return clientWithHttpLookup.getLookup();
        } else {
            return clientWitBinaryLookup.getLookup();
        }
    }

    @DataProvider(name = "isUsingHttpLookup")
    public Object[][] isUsingHttpLookup() {
        return new Object[][]{
            {true},
            {false}
        };
    }

    @Test(dataProvider = "isUsingHttpLookup")
    public void testGetTopicsOfGetTopicsResult(boolean isUsingHttpLookup) throws Exception {
        LookupService lookupService = getLookupService(isUsingHttpLookup);
        String nonPartitionedTopic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(nonPartitionedTopic);
        String partitionedTopic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createPartitionedTopic(partitionedTopic, 3);
        String nonPersistentTopic = BrokerTestUtil.newUniqueName("non-persistent://public/default/tp");

        // Verify the new method "GetTopicsResult.getTopics" works as expected.
        Collection<String> topics = lookupService.getTopicsUnderNamespace(NamespaceName.get("public/default"),
                Mode.PERSISTENT, "public/default/.*", null).join().getTopics();
        assertTrue(topics.contains(nonPartitionedTopic));
        assertTrue(topics.contains(partitionedTopic));
        assertFalse(topics.contains(nonPersistentTopic));
        assertFalse(topics.contains(TopicName.get(partitionedTopic).getPartition(0).toString()));
        // Verify the new method "GetTopicsResult.nonPartitionedOrPartitionTopics" works as expected.
        Collection<String> nonPartitionedOrPartitionTopics =
                lookupService.getTopicsUnderNamespace(NamespaceName.get("public/default"),
                Mode.PERSISTENT, "public/default/.*", null).join()
                .getNonPartitionedOrPartitionTopics();
        assertTrue(nonPartitionedOrPartitionTopics.contains(nonPartitionedTopic));
        assertFalse(nonPartitionedOrPartitionTopics.contains(partitionedTopic));
        assertFalse(nonPartitionedOrPartitionTopics.contains(nonPersistentTopic));
        assertTrue(nonPartitionedOrPartitionTopics.contains(TopicName.get(partitionedTopic).getPartition(0)
                .toString()));
        assertTrue(nonPartitionedOrPartitionTopics.contains(TopicName.get(partitionedTopic).getPartition(1)
                .toString()));
        assertTrue(nonPartitionedOrPartitionTopics.contains(TopicName.get(partitionedTopic).getPartition(2)
                .toString()));

        // Cleanup.
        admin.topics().deletePartitionedTopic(partitionedTopic, false);
        admin.topics().delete(nonPartitionedTopic, false);
    }

}
