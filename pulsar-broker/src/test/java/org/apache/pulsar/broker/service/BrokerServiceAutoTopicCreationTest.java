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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerServiceAutoTopicCreationTest extends BrokerTestBase{

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanupTest() throws Exception {
        pulsar.getAdminClient().namespaces().removeAutoTopicCreation("prop/ns-abc");
    }

    @Test
    public void testAutoNonPartitionedTopicCreation() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("non-partitioned");

        final String topicString = "persistent://prop/ns-abc/non-partitioned-topic";
        final String subscriptionName = "non-partitioned-topic-sub";
        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoNonPartitionedTopicCreationOnProduce() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("non-partitioned");

        final String topicString = "persistent://prop/ns-abc/non-partitioned-topic-2";
        pulsarClient.newProducer().topic(topicString).create();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoPartitionedTopicCreation() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partitioned");
        pulsar.getConfiguration().setDefaultNumPartitions(3);

        final String topicString = "persistent://prop/ns-abc/partitioned-topic";
        final String subscriptionName = "partitioned-topic-sub";
        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
        for (int i = 0; i < 3; i++) {
            assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString + "-partition-" + i));
        }
    }

    @Test
    public void testAutoPartitionedTopicCreationOnProduce() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partitioned");
        pulsar.getConfiguration().setDefaultNumPartitions(3);

        final String topicString = "persistent://prop/ns-abc/partitioned-topic-1";
        pulsarClient.newProducer().topic(topicString).create();

        assertTrue(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
        for (int i = 0; i < 3; i++) {
            assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString + "-partition-" + i));
        }
    }

    @Test
    public void testAutoTopicCreationDisable() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);

        final String topicString = "persistent://prop/ns-abc/test-topic";
        final String subscriptionName = "test-topic-sub";
        try {
            pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();
            fail("Subscribe operation should have failed");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException);
        }
        assertFalse(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoTopicCreationDisableIfNonPartitionedTopicAlreadyExist() throws Exception {
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partitioned");
        pulsar.getConfiguration().setDefaultNumPartitions(3);

        final String topicString = "persistent://prop/ns-abc/test-topic-2";
        final String subscriptionName = "partitioned-topic-sub";
        admin.topics().createNonPartitionedTopic(topicString);
        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();

        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
        for (int i = 0; i < 3; i++) {
            assertFalse(admin.namespaces().getTopics("prop/ns-abc").contains(topicString + "-partition-" + i));
        }
        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
    }

    /**
     * CheckAllowAutoCreation's default value is false.
     * So using getPartitionedTopicMetadata() directly will not produce partitioned topic
     * even if the option to automatically create partitioned topic is configured
     */
    @Test
    public void testGetPartitionedMetadataWithoutCheckAllowAutoCreation() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partitioned");
        pulsar.getConfiguration().setDefaultNumPartitions(3);

        final String topicString = "persistent://prop/ns-abc/test-topic-3";
        try {
            admin.topics().getPartitionedTopicMetadata(topicString);
        } catch (PulsarAdminException.NotFoundException expected) {
        }
        assertFalse(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoCreationNamespaceAllowOverridesBroker() throws Exception {
        final String topicString = "persistent://prop/ns-abc/test-topic-4";
        final String subscriptionName = "test-topic-sub-4";
        final TopicName topicName = TopicName.get(topicString);
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(),
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType(TopicType.NON_PARTITIONED.toString())
                        .build());

        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();
        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoCreationNamespaceDisallowOverridesBroker() throws Exception {
        final String topicString = "persistent://prop/ns-abc/test-topic-5";
        final String subscriptionName = "test-topic-sub-5";
        final TopicName topicName = TopicName.get(topicString);
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(),
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(false)
                        .build());

        try {
            pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();
            fail("Subscribe operation should have failed");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException);
        }
        assertFalse(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoCreationNamespaceOverrideAllowsPartitionedTopics() throws Exception {
        final String topicString = "persistent://prop/ns-abc/partitioned-test-topic-6";
        final TopicName topicName = TopicName.get(topicString);

        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(),
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType(TopicType.PARTITIONED.toString())
                        .defaultNumPartitions(4)
                        .build());

        final String subscriptionName = "test-topic-sub-6";
        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
        for (int i = 0; i < 4; i++) {
            assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString + "-partition-" + i));
        }
    }

    @Test
    public void testAutoCreationNamespaceOverridesTopicTypePartitioned() throws Exception {
        final String topicString = "persistent://prop/ns-abc/partitioned-test-topic-7";
        final TopicName topicName = TopicName.get(topicString);

        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("non-partitioned");
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(),
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType(TopicType.PARTITIONED.toString())
                        .defaultNumPartitions(3)
                        .build());

        final String subscriptionName = "test-topic-sub-7";
        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
        for (int i = 0; i < 3; i++) {
            assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString + "-partition-" + i));
        }
    }

    @Test
    public void testAutoCreationNamespaceOverridesTopicTypeNonPartitioned() throws Exception {
        final String topicString = "persistent://prop/ns-abc/partitioned-test-topic-8";
        final TopicName topicName = TopicName.get(topicString);

        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partitioned");
        pulsar.getConfiguration().setDefaultNumPartitions(2);
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(),
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType(TopicType.NON_PARTITIONED.toString())
                        .build());

        final String subscriptionName = "test-topic-sub-8";
        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoCreationNamespaceOverridesDefaultNumPartitions() throws Exception {
        final String topicString = "persistent://prop/ns-abc/partitioned-test-topic-9";
        final TopicName topicName = TopicName.get(topicString);

        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partitioned");
        pulsar.getConfiguration().setDefaultNumPartitions(2);
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(),
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType(TopicType.PARTITIONED.toString())
                        .defaultNumPartitions(4)
                        .build());

        final String subscriptionName = "test-topic-sub-9";

        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
        for (int i = 0; i < 4; i++) {
            assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString + "-partition-" + i));
        }
    }

    @Test
    public void testAutoCreationNamespaceAllowOverridesBrokerOnProduce() throws Exception {
        final String topicString = "persistent://prop/ns-abc/test-topic-10";
        final TopicName topicName = TopicName.get(topicString);
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(),
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType(TopicType.NON_PARTITIONED.toString())
                        .build());

        pulsarClient.newProducer().topic(topicString).create();
        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }


    @Test
    public void testNotAllowSubscriptionTopicCreation() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        String topicName = "persistent://prop/ns-abc/non-partitioned-topic" + System.currentTimeMillis();
        String subscriptionName = "non-partitioned-topic-sub";

        try {
            admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
            fail("should fail to create subscription once not allowAutoTopicCreation");
        } catch (Exception e) {
            // expected
        }

        try {
            admin.topics().createSubscription(topicName + "-partition-0",
                    subscriptionName, MessageId.earliest);
            fail("should fail to create subscription once not allowAutoTopicCreation");
        } catch (Exception e) {
            // expected
        }

        assertFalse(admin.namespaces().getTopics("prop/ns-abc").contains(topicName));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicName));

        try {
            admin.topics().createNonPartitionedTopic(topicName);
            admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
        } catch (Exception e) {
            // expected
            fail("should success to create subscription once topic created");
        }

        try {
            String partitionTopic = "persistent://prop/ns-abc/partitioned-topic" + System.currentTimeMillis();
            admin.topics().createPartitionedTopic(partitionTopic, 1);
            admin.topics().createSubscription(partitionTopic + "-partition-0", subscriptionName, MessageId.earliest);
        } catch (Exception e) {
            // expected
            fail("should success to create subscription once topic created");
        }

    }

    @Test
    public void testMaxNumPartitionsPerPartitionedTopicTopicCreation() {
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partitioned");
        pulsar.getConfiguration().setDefaultNumPartitions(3);
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(2);

        final String topicString = "persistent://prop/ns-abc/partitioned-test-topic-11";
        final String subscriptionName = "test-topic-sub-11";

        try {
            pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();
            fail("should throw exception when number of partitions exceed than max partitions");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException);
        }
    }

    @Test
    public void testAutoCreationOfSystemTopicTransactionBufferSnapshot() throws Exception {
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        pulsar.getConfiguration().setSystemTopicEnabled(true);

        final String topicString = "persistent://prop/ns-abc/" + EventsTopicNames.TRANSACTION_BUFFER_SNAPSHOT;

        pulsarClient.newProducer().topic(topicString).create();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoCreationOfSystemTopicNamespaceEvents() throws Exception {
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        pulsar.getConfiguration().setSystemTopicEnabled(true);

        final String topicString = "persistent://prop/ns-abc/" + EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;

        pulsarClient.newProducer().topic(topicString).create();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }
}
