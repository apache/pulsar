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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-replication")
public class ReplicationTopicGcTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @DataProvider(name = "topicTypes")
    public Object[][] topicTypes() {
        return new Object[][]{
                {TopicType.NON_PARTITIONED},
                {TopicType.PARTITIONED}
        };
    }

    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        config.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        config.setBrokerDeleteInactiveTopicsEnabled(true);
        config.setBrokerDeleteInactivePartitionedTopicMetadataEnabled(true);
        config.setBrokerDeleteInactiveTopicsFrequencySeconds(5);
        config.setBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(5);
        config.setReplicationPolicyCheckDurationSeconds(1);
    }

    @Test(dataProvider = "topicTypes")
    public void testTopicGC(TopicType topicType) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        final String schemaId = TopicName.get(topicName).getSchemaName();
        final String subTopic = topicType == TopicType.NON_PARTITIONED ? topicName
                : TopicName.get(topicName).getPartition(0).toString();
        if (topicType == TopicType.NON_PARTITIONED) {
            admin1.topics().createNonPartitionedTopic(topicName);
        } else {
            admin1.topics().createPartitionedTopic(topicName, 1);
        }

        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).create();
        // Wait for replicator started.
        waitReplicatorStarted(subTopic);

        // Trigger a topic level policies.
        PublishRate publishRate = new PublishRate(1000, 1024 * 1024);
        admin1.topicPolicies().setPublishRate(topicName, publishRate);
        admin2.topicPolicies().setPublishRate(topicName, publishRate);
        // Write a schema.
        // Since there is a producer registered one the source cluster, skipped to write a schema.
        admin2.schemas().createSchema(topicName, Schema.STRING.getSchemaInfo());

        // Trigger GC through close all clients.
        producer1.close();
        // Verify: all resources were deleted.
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            // sub topic.
            assertFalse(pulsar1.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(subTopic)).get());
            assertFalse(pulsar2.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(subTopic)).get());
            // partitioned topic.
            assertFalse(pulsar1.getPulsarResources().getNamespaceResources()
                    .getPartitionedTopicResources().partitionedTopicExists(TopicName.get(topicName)));
            assertFalse(pulsar2.getPulsarResources().getNamespaceResources()
                    .getPartitionedTopicResources().partitionedTopicExists(TopicName.get(topicName)));
            // topic policies.
            assertTrue(pulsar1.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(topicName))
                    .get().isEmpty());
            assertTrue(pulsar2.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(topicName))
                    .get().isEmpty());
            // schema.
            assertTrue(CollectionUtils.isEmpty(pulsar1.getSchemaStorage().getAll(schemaId).get()));
            assertTrue(CollectionUtils.isEmpty(pulsar2.getSchemaStorage().getAll(schemaId).get()));
        });
    }

    @Test(dataProvider = "topicTypes")
    public void testRemoteClusterStillConsumeAfterCurrentClusterGc(TopicType topicType) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        final String subscription = "s1";
        final String schemaId = TopicName.get(topicName).getSchemaName();
        final String subTopic = topicType == TopicType.NON_PARTITIONED ? topicName
                : TopicName.get(topicName).getPartition(0).toString();
        if (topicType == TopicType.NON_PARTITIONED) {
            admin1.topics().createNonPartitionedTopic(topicName);
        } else {
            admin1.topics().createPartitionedTopic(topicName, 1);
        }

        // Wait for replicator started.
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).create();
        waitReplicatorStarted(subTopic);
        admin2.topics().createSubscription(topicName, subscription, MessageId.earliest);

        if (usingGlobalZK) {
            admin2.topics().setReplicationClusters(topicName, Collections.singletonList(cluster2));
            waitReplicatorStopped(pulsar2, pulsar1, subTopic);
        }

        // Send a message
        producer1.send("msg-1");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin2.topics().getStats(subTopic).getSubscriptions().get(subscription).getMsgBacklog(), 1);
        });

        // Trigger GC through close all clients.
        producer1.close();
        // Verify: the topic was removed on the source cluster.
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            // sub topic.
            assertFalse(pulsar1.getPulsarResources().getTopicResources()
                    .persistentTopicExists(TopicName.get(subTopic)).get());
            // partitioned topic.
            assertFalse(pulsar1.getPulsarResources().getNamespaceResources()
                    .getPartitionedTopicResources().partitionedTopicExists(TopicName.get(topicName)));
            // topic policies.
            assertTrue(pulsar1.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(topicName))
                    .get().isEmpty());
            // schema.
            assertTrue(CollectionUtils.isEmpty(pulsar1.getSchemaStorage().getAll(schemaId).get()));
        });

        // Verify: consumer still can consume messages from the remote cluster.
        org.apache.pulsar.client.api.Consumer<String> consumer =
                client2.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subscription).subscribe();
        Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getValue(), "msg-1");

        // Cleanup.
        consumer.close();
        if (topicType == TopicType.NON_PARTITIONED) {
            admin2.topics().delete(topicName);
        } else {
            admin2.topics().deletePartitionedTopic(topicName);
        }
    }
}
