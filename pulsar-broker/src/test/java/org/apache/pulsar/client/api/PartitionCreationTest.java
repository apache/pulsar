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
package org.apache.pulsar.client.api;

import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class PartitionCreationTest extends ProducerConsumerBase {

    @DataProvider(name = "topicDomainProvider")
    public Object[][] topicDomainProvider() {
        return new Object[][] {
                { TopicDomain.persistent },
                { TopicDomain.non_persistent }
        };
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setManagedLedgerCacheEvictionIntervalMs(10000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(dataProvider = "topicDomainProvider", timeOut = 60000)
    public void testCreateConsumerForPartitionedTopicWhenDisableTopicAutoCreation(TopicDomain domain)
            throws PulsarAdminException, PulsarClientException {
        conf.setAllowAutoTopicCreation(domain.equals(TopicDomain.non_persistent));
        final String topic = domain.value() + "://public/default/testCreateConsumerWhenDisableTopicAutoCreation";
        admin.topics().createPartitionedTopic(topic, 3);
        Assert.assertNotNull(pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1").subscribe());
    }

    @Test(dataProvider = "topicDomainProvider", timeOut = 60000)
    public void testCreateConsumerForNonPartitionedTopicWhenDisableTopicAutoCreation(TopicDomain domain)
            throws PulsarClientException {
        conf.setAllowAutoTopicCreation(false);
        final String topic = domain.value() + "://public/default/"
                + "testCreateConsumerForNonPartitionedTopicWhenDisableTopicAutoCreation";
        try {
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1").subscribe();
            if (domain == TopicDomain.persistent) {
                Assert.fail("should be failed");
            } else {
                // passed non persistent topic here since we can not avoid auto creation on non persistent topic now.
                Assert.assertNotNull(consumer);
            }
        } catch (PulsarClientException.TopicDoesNotExistException | PulsarClientException.NotFoundException e) {
            //ok
        }
    }

    @Test(dataProvider = "topicDomainProvider", timeOut = 60000)
    public void testCreateConsumerForPartitionedTopicWhenEnableTopicAutoCreation(TopicDomain domain)
            throws PulsarAdminException, PulsarClientException {
        conf.setAllowAutoTopicCreation(true);
        final String topic = domain.value() + "://public/default/"
                + "testCreateConsumerForPartitionedTopicWhenEnableTopicAutoCreation";
        admin.topics().createPartitionedTopic(topic, 3);
        Assert.assertNotNull(pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1").subscribe());
    }

    @Test(dataProvider = "topicDomainProvider", timeOut = 60000)
    public void testCreateConsumerForNonPartitionedTopicWhenEnableTopicAutoCreation(TopicDomain domain)
            throws PulsarClientException {
        conf.setAllowAutoTopicCreation(true);
        final String topic = domain.value() + "://public/default/"
                + "testCreateConsumerForNonPartitionedTopicWhenEnableTopicAutoCreation";
        Assert.assertNotNull(pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1").subscribe());
    }

    @Test(timeOut = 60000)
    public void testCreateConsumerForPartitionedTopicUpdateWhenDisableTopicAutoCreation() throws Exception {
        conf.setAllowAutoTopicCreation(false);
        final String topic = "testCreateConsumerForPartitionedTopicUpdateWhenDisableTopicAutoCreation-"
                + System.currentTimeMillis();
        admin.topics().createPartitionedTopic(topic, 3);
        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic).subscriptionName("sub-1").subscribe();
        Assert.assertNotNull(consumer);
        Assert.assertEquals(consumer.getConsumers().size(), 3);
        consumer.close();
        admin.topics().updatePartitionedTopic(topic, 5);
        consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic).subscriptionName("sub-1").subscribe();
        Assert.assertNotNull(consumer);
        Assert.assertEquals(consumer.getConsumers().size(), 5);
    }

    @Test
    public void testGetPoliciesIfPartitionsNotCreated() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        int numPartitions = 3;
        // simulate partitioned topic without partitions
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopicAsync(TopicName.get(topic),
                        new PartitionedTopicMetadata(numPartitions)).join();
        // Verify: the command will not get an topic not found error.
        admin.topics().getReplicationClusters(topic, true);
        // cleanup.
        admin.topics().deletePartitionedTopic(topic);
    }

    @DataProvider(name = "restCreateMissedPartitions")
    public Object[] restCreateMissedPartitions() {
        return new Object[] { true, false };
    }

    @Test(timeOut = 60000, dataProvider = "restCreateMissedPartitions")
    public void testCreateMissedPartitions(boolean useRestApi)
            throws PulsarAdminException, PulsarClientException, MetadataStoreException {
        conf.setAllowAutoTopicCreation(false);
        final String topic = "testCreateMissedPartitions-useRestApi-" + useRestApi;
        int numPartitions = 3;
        // simulate partitioned topic without partitions
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopicAsync(TopicName.get(topic),
                new PartitionedTopicMetadata(numPartitions)).join();
        Assert.assertEquals(admin.topics().getList("public/default").stream()
            .filter(tp -> TopicName.get(tp).getPartitionedTopicName().endsWith(topic)).toList().size(), 0);
        if (useRestApi) {
            admin.topics().createMissedPartitions(topic);
        } else {
            final TopicName topicName = TopicName.get(topic);
            for (int i = 0; i < numPartitions; i++) {
                admin.topics().createNonPartitionedTopic(topicName.getPartition(i).toString());
            }
        }
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1").subscribe();
        Assert.assertNotNull(consumer);
        Assert.assertTrue(consumer instanceof MultiTopicsConsumerImpl);
        Assert.assertEquals(((MultiTopicsConsumerImpl) consumer).getConsumers().size(), 3);
    }

}
