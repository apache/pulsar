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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.client.api.PulsarClientException;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BrokerServiceAutoTopicCreationTest extends BrokerTestBase{
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAutoNonPartitionedTopicCreation() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("non-partition");

        final String topicName = "persistent://prop/ns-abc/non-partitioned-topic";
        final String subscriptionName = "non-partitioned-topic-sub";
        pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicName));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicName));
    }

    @Test
    public void testAutoPartitionedTopicCreation() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partition");
        pulsar.getConfiguration().setAllowAutoTopicCreationNumPartitions(3);

        final String topicName = "persistent://prop/ns-abc/partitioned-topic";
        final String subscriptionName = "partitioned-topic-sub";
        pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicName));
        for (int i = 0; i < 3; i++) {
            assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicName + "-partition-" + i));
        }
    }

    @Test
    public void testAutoTopicCreationDisable() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);

        final String topicName = "persistent://prop/ns-abc/test-topic";
        final String subscriptionName = "test-topic-sub";
        try {
            pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException);
        }
        assertFalse(admin.namespaces().getTopics("prop/ns-abc").contains(topicName));
    }
}
