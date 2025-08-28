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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableViewImpl.TOPIC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.ListNamespaceTopicsOptions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.awaitility.Awaitility;
import org.testng.Assert;
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
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);

        final String topicString = "persistent://prop/ns-abc/non-partitioned-topic";
        final String subscriptionName = "non-partitioned-topic-sub";
        pulsarClient.newConsumer().topic(topicString).subscriptionName(subscriptionName).subscribe();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoNonPartitionedTopicCreationOnProduce() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);

        final String topicString = "persistent://prop/ns-abc/non-partitioned-topic-2";
        pulsarClient.newProducer().topic(topicString).create();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc").contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoPartitionedTopicCreation() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
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
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
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
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
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
        admin.topics().delete(topicString, true);
    }

    /**
     * CheckAllowAutoCreation's default value is false.
     * So using getPartitionedTopicMetadata() directly will not produce partitioned topic
     * even if the option to automatically create partitioned topic is configured
     */
    @Test
    public void testGetPartitionedMetadataWithoutCheckAllowAutoCreation() throws Exception{
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
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
        AutoTopicCreationOverride autoTopicCreationOverride = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.NON_PARTITIONED.toString())
                .build();
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(), autoTopicCreationOverride);
        Assert.assertEquals(pulsar.getAdminClient().namespaces().getAutoTopicCreation(topicName.getNamespace()),
                autoTopicCreationOverride);

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
        AutoTopicCreationOverride autoTopicCreationOverride = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(false)
                .build();
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(), autoTopicCreationOverride);
        Assert.assertEquals(pulsar.getAdminClient().namespaces().getAutoTopicCreation(topicName.getNamespace()),
                autoTopicCreationOverride);

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
        AutoTopicCreationOverride autoTopicCreationOverride = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .defaultNumPartitions(4)
                .build();
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(), autoTopicCreationOverride);
        Assert.assertEquals(pulsar.getAdminClient().namespaces().getAutoTopicCreation(topicName.getNamespace()),
                autoTopicCreationOverride);

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
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
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
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
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
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
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
    public void testAutoCreationNamespaceOverridesSubscriptionTopicCreation() throws Exception {
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        String topicString = "persistent://prop/ns-abc/non-partitioned-topic" + System.currentTimeMillis();
        String subscriptionName = "non-partitioned-topic-sub";
        final TopicName topicName = TopicName.get(topicString);
        pulsar.getAdminClient().namespaces().setAutoTopicCreation(topicName.getNamespace(),
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType(TopicType.NON_PARTITIONED.toString())
                        .build());

        admin.topics().createSubscription(topicString, subscriptionName, MessageId.earliest);
    }

    @Test
    public void testMaxNumPartitionsPerPartitionedTopicTopicCreation() {
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
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

        final String topicString = "persistent://prop/ns-abc/" + SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT;

        pulsarClient.newProducer().topic(topicString).create();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc",
                ListNamespaceTopicsOptions.builder().includeSystemTopic(true).build()).contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testAutoCreationOfSystemTopicNamespaceEvents() throws Exception {
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);

        final String topicString = "persistent://prop/ns-abc/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicString).create();

        assertTrue(admin.namespaces().getTopics("prop/ns-abc",
                ListNamespaceTopicsOptions.builder().includeSystemTopic(true).build()).contains(topicString));
        assertFalse(admin.topics().getPartitionedTopicList("prop/ns-abc").contains(topicString));
    }

    @Test
    public void testDynamicConfigurationTopicAutoCreationDisable() throws Exception {
        // test disable AllowAutoTopicCreation
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        admin.brokers().updateDynamicConfiguration("allowAutoTopicCreation", "false");
        final String namespaceName = "prop/ns-abc";
        final String topic = "persistent://" + namespaceName + "/test-dynamicConfiguration-topic-auto-creation-"
                + UUID.randomUUID();
        try {
            pulsarClient.newProducer().topic(topic).create();
            fail("expected a topic not found exception");
        } catch (PulsarClientException.NotFoundException e) {
            // expected.
        }
    }

    @Test
    public void testDynamicConfigurationTopicAutoCreationNonPartitioned()
            throws PulsarAdminException, PulsarClientException {
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        final String namespaceName = "prop/ns-abc";
        final String topic = "persistent://" + namespaceName + "/test-dynamicConfiguration-topic-auto-creation-"
                + UUID.randomUUID();
        // test enable AllowAutoTopicCreation, non-partitioned
        admin.brokers().updateDynamicConfiguration("allowAutoTopicCreation", "true");
        admin.brokers().updateDynamicConfiguration("allowAutoTopicCreationType", "non-partitioned");
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        List<String> topics = admin.topics().getList(namespaceName).stream().filter(tp -> {
            return TopicName.get(tp).getPartitionedTopicName().equals(topic);
        }).toList();
        List<String> partitionedTopicList =
                admin.topics().getPartitionedTopicList(namespaceName).stream().filter(tp -> {
                    return TopicName.get(tp).getPartitionedTopicName().equals(topic);
                }).toList();
        assertEquals(topics.size(), 1);
        assertEquals(partitionedTopicList.size(), 0);
        producer.close();
        admin.topics().delete(topic);
    }

    @Test
    public void testDynamicConfigurationTopicAutoCreationPartitioned()
            throws PulsarAdminException, PulsarClientException {
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(0);
        final String namespaceName = "prop/ns-abc";
        final String topic = "persistent://" + namespaceName + "/test-dynamicConfiguration-topic-auto-creation-"
                + UUID.randomUUID();
        // test enable AllowAutoTopicCreation, partitioned
        admin.brokers().updateDynamicConfigurationAsync("allowAutoTopicCreation", "true");
        admin.brokers().updateDynamicConfiguration("maxNumPartitionsPerPartitionedTopic", "6");
        admin.brokers().updateDynamicConfiguration("allowAutoTopicCreationType", "partitioned");
        admin.brokers().updateDynamicConfiguration("defaultNumPartitions", "4");
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(pulsar.getConfiguration().isAllowAutoTopicCreation());
            assertEquals(pulsar.getConfiguration().getMaxNumPartitionsPerPartitionedTopic(), 6);
            assertEquals(pulsar.getConfiguration().getAllowAutoTopicCreationType(), TopicType.PARTITIONED);
            assertEquals(pulsar.getConfiguration().getDefaultNumPartitions(), 4);

        });
        Producer<byte[]> producer  = pulsarClient.newProducer().topic(topic).create();
        List<String> topics = admin.topics().getList(namespaceName).stream().filter(tp -> {
            return TopicName.get(tp).getPartitionedTopicName().equals(topic);
        }).toList();
        List<String> partitionedTopicList =
                admin.topics().getPartitionedTopicList(namespaceName).stream().filter(tp -> {
                    return TopicName.get(tp).getPartitionedTopicName().equals(topic);
                }).toList();
        PartitionedTopicMetadata partitionedTopicMetadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(topics.size(), 4);
        assertEquals(partitionedTopicList.size(), 1);
        assertEquals(partitionedTopicMetadata.partitions, 4);
        producer.close();
        for (String t : topics) {
            admin.topics().delete(t);
        }
    }

    @Test
    public void testDynamicConfigurationTopicAutoCreationPartitionedWhenDefaultMoreThanMax()
            throws PulsarAdminException, PulsarClientException {
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(0);
        final String namespaceName = "prop/ns-abc";
        final String topic = "persistent://" + namespaceName + "/test-dynamicConfiguration-topic-auto-creation-"
                + UUID.randomUUID();
        // test enable AllowAutoTopicCreation,
        // partitioned when maxNumPartitionsPerPartitionedTopic < defaultNumPartitions
        admin.brokers().updateDynamicConfiguration("maxNumPartitionsPerPartitionedTopic", "2");
        admin.brokers().updateDynamicConfiguration("defaultNumPartitions", "6");
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        List<String> topics = admin.topics().getList(namespaceName).stream().filter(tp -> {
            return TopicName.get(tp).getPartitionedTopicName().equals(topic);
        }).toList();
        List<String> partitionedTopicList =
                admin.topics().getPartitionedTopicList(namespaceName).stream().filter(tp -> {
                    return TopicName.get(tp).getPartitionedTopicName().equals(topic);
                }).toList();
        PartitionedTopicMetadata partitionedTopicMetadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(topics.size(), 2);
        assertEquals(partitionedTopicList.size(), 1);
        assertEquals(partitionedTopicMetadata.partitions, 2);
        producer.close();
        for (String t : topics) {
            admin.topics().delete(t);
        }

        // set maxNumPartitionsPerPartitionedTopic, make maxNumPartitionsPerPartitionedTopic < defaultNumPartitions
        admin.brokers().updateDynamicConfiguration("maxNumPartitionsPerPartitionedTopic", "1");
        // Make sure the dynamic cache is updated to prevent the flaky test.
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.brokers().getAllDynamicConfigurations()
                        .get("maxNumPartitionsPerPartitionedTopic"), "1"));
        final String topic2 = "persistent://" + namespaceName + "/test-dynamicConfiguration-topic-auto-creation-"
                + UUID.randomUUID();
        producer = pulsarClient.newProducer().topic(topic2).create();
        List<String> topics2 = admin.topics().getList(namespaceName).stream().filter(tp -> {
            return TopicName.get(tp).getPartitionedTopicName().equals(topic2);
        }).toList();
        assertEquals(topics2.size(), 1);
        producer.close();
        for (String t : topics2) {
            admin.topics().delete(t);
        }
    }

    @Test
    public void testExtensibleLoadManagerImplInternalTopicAutoCreations()
            throws PulsarAdminException, PulsarClientException {
        pulsar.getConfiguration().setAllowAutoTopicCreation(true);
        pulsar.getConfiguration().setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        pulsar.getConfiguration().setDefaultNumPartitions(3);
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(5);
        final String namespaceName = NamespaceName.SYSTEM_NAMESPACE.toString();
        TenantInfoImpl tenantInfo = new TenantInfoImpl();
        tenantInfo.setAllowedClusters(Set.of(configClusterName));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace(namespaceName);
        admin.topics().createNonPartitionedTopic(TOPIC);
        admin.topics().createNonPartitionedTopic(ExtensibleLoadManagerImpl.BROKER_LOAD_DATA_STORE_TOPIC);
        admin.topics().createNonPartitionedTopic(ExtensibleLoadManagerImpl.TOP_BUNDLES_LOAD_DATA_STORE_TOPIC);

        // clear the topics to test the auto creation of non-persistent topics.
        final var topics = pulsar.getBrokerService().getTopics();
        final var oldTopics = topics.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                Map.Entry::getValue));
        topics.clear();

        // The created persistent topic correctly can be found by
        // pulsar.getPulsarResources().getTopicResources().persistentTopicExists(topic);
        Producer producer = pulsarClient.newProducer().topic(TOPIC).create();

        // The created non-persistent topics cannot be found, as we did topics.clear()
        try {
            pulsarClient.newProducer().topic(ExtensibleLoadManagerImpl.BROKER_LOAD_DATA_STORE_TOPIC).create();
            Assert.fail("Create should have failed.");
        } catch (PulsarClientException.TopicDoesNotExistException | PulsarClientException.NotFoundException e) {
            // expected
        }
        try {
            pulsarClient.newProducer().topic(ExtensibleLoadManagerImpl.TOP_BUNDLES_LOAD_DATA_STORE_TOPIC).create();
            Assert.fail("Create should have failed.");
        } catch (PulsarClientException.TopicDoesNotExistException | PulsarClientException.NotFoundException e) {
            // expected
        }

        oldTopics.forEach((key, val) -> topics.put(key, val));

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> partitionedTopicList = admin.topics().getPartitionedTopicList(namespaceName);
            assertEquals(partitionedTopicList.size(), 0);
        });

        producer.close();
        admin.namespaces().deleteNamespace(namespaceName);
        admin.tenants().deleteTenant("pulsar");

    }

    @Test
    public void testCreateTopicAfterGC() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/tp");
        final String topicP0 = TopicName.get(topic).getPartition(0).toString();
        // Disable topic auto-creation.
        boolean originalAllowAutoTopicCreation = pulsar.getConfiguration().isAllowAutoTopicCreation();
        boolean originalDeleteInactiveTopics =
                pulsar.getConfiguration().isBrokerDeleteInactiveTopicsEnabled();
        boolean originalDeleteInactivePartitionedTopicMetadataE =
                pulsar.getConfiguration().isBrokerDeleteInactivePartitionedTopicMetadataEnabled();
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        pulsar.getConfiguration().setBrokerDeleteInactiveTopicsEnabled(true);
        pulsar.getConfiguration().setBrokerDeleteInactivePartitionedTopicMetadataEnabled(false);
        // Create partitioned topic.
        Assert.assertThrows(PulsarClientException.NotFoundException.class,
                () -> pulsarClient.newProducer().topic(topic).create());
        admin.topics().createPartitionedTopic(topic, 1);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicP0, false).join().get();
        // Enable topic GC.
        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true);
        admin.topicPolicies().setInactiveTopicPolicies(topic, inactiveTopicPolicies);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(persistentTopic.topicPolicies.getInactiveTopicPolicies().getTopicValue()
                    .getInactiveTopicDeleteMode(), InactiveTopicDeleteMode.delete_when_no_subscriptions);
            assertEquals(persistentTopic.topicPolicies.getInactiveTopicPolicies().getTopicValue()
                    .getMaxInactiveDurationSeconds(), 1);
            assertTrue(persistentTopic.topicPolicies.getInactiveTopicPolicies().getTopicValue()
                    .isDeleteWhileInactive());
        });
        // Trigger topic GC.
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            persistentTopic.checkGC();
            assertFalse(pulsar.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(topicP0)).get());
        });

        // Verify: the missed partition can be loaded up since the partitioned topic metadata exists.
        pulsarClient.newProducer().topic(topic).create().close();

        // cleanup.
        admin.topics().deletePartitionedTopic(topic);
        pulsar.getConfiguration().setAllowAutoTopicCreation(originalAllowAutoTopicCreation);
        pulsar.getConfiguration().setBrokerDeleteInactiveTopicsEnabled(originalDeleteInactiveTopics);
        pulsar.getConfiguration().setBrokerDeleteInactivePartitionedTopicMetadataEnabled(
                originalDeleteInactivePartitionedTopicMetadataE);
    }
}
