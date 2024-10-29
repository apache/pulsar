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
package org.apache.pulsar.broker.resourcegroup;

import static org.testng.Assert.assertNotNull;
import com.google.common.collect.Sets;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupService.ResourceGroupUsageStatsType;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ResourceGroupUsageAggregationOnTopicLevelTest extends ProducerConsumerBase {

    private final String TenantName = "pulsar-test";
    private final String NsName = "test";
    private final String TenantAndNsName = TenantName + "/" + NsName;
    private final String TestProduceConsumeTopicName = "/test/prod-cons-topic";
    private final String PRODUCE_CONSUME_PERSISTENT_TOPIC = "persistent://" + TenantAndNsName + TestProduceConsumeTopicName;
    private final String PRODUCE_CONSUME_NON_PERSISTENT_TOPIC =
            "non-persistent://" + TenantAndNsName + TestProduceConsumeTopicName;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        this.conf.setAllowAutoTopicCreation(true);

        final String clusterName = "test";
        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant(TenantName,
                new TenantInfoImpl(Sets.newHashSet("fakeAdminRole"), Sets.newHashSet(clusterName)));
        admin.namespaces().createNamespace(TenantAndNsName);
        admin.namespaces().setNamespaceReplicationClusters(TenantAndNsName, Sets.newHashSet(clusterName));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPersistentTopicProduceConsumeUsageOnRG() throws Exception {
        testProduceConsumeUsageOnRG(PRODUCE_CONSUME_PERSISTENT_TOPIC);
    }
    
    @Test
    public void testNonPersistentTopicProduceConsumeUsageOnRG() throws Exception {
        testProduceConsumeUsageOnRG(PRODUCE_CONSUME_NON_PERSISTENT_TOPIC);
    }

    private void testProduceConsumeUsageOnRG(String topicString) throws Exception {
        ResourceQuotaCalculator dummyQuotaCalc = new ResourceQuotaCalculator() {
            @Override
            public boolean needToReportLocalUsage(long currentBytesUsed, long lastReportedBytes,
                                                  long currentMessagesUsed, long lastReportedMessages,
                                                  long lastReportTimeMSecsSinceEpoch) {
                return false;
            }

            @Override
            public long computeLocalQuota(long confUsage, long myUsage, long[] allUsages) {
                return 0;
            }
        };

        @Cleanup
        ResourceUsageTopicTransportManager transportMgr = new ResourceUsageTopicTransportManager(pulsar);
        @Cleanup
        ResourceGroupService rgs = new ResourceGroupService(pulsar, TimeUnit.MILLISECONDS, transportMgr,
                dummyQuotaCalc);

        String activeRgName = "runProduceConsume";
        ResourceGroup activeRG;

        ResourceUsagePublisher ruP = new ResourceUsagePublisher() {
            @Override
            public String getID() {
                return rgs.resourceGroupGet(activeRgName).resourceGroupName;
            }

            @Override
            public void fillResourceUsage(ResourceUsage resourceUsage) {
                rgs.resourceGroupGet(activeRgName).rgFillResourceUsage(resourceUsage);
            }
        };

        ResourceUsageConsumer ruC = new ResourceUsageConsumer() {
            @Override
            public String getID() {
                return rgs.resourceGroupGet(activeRgName).resourceGroupName;
            }

            @Override
            public void acceptResourceUsage(String broker, ResourceUsage resourceUsage) {
                rgs.resourceGroupGet(activeRgName).rgResourceUsageListener(broker, resourceUsage);
            }
        };

        org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        rgConfig.setPublishRateInBytes(1500L);
        rgConfig.setPublishRateInMsgs(100);
        rgConfig.setDispatchRateInBytes(4000L);
        rgConfig.setPublishRateInMsgs(500);

        rgs.resourceGroupCreate(activeRgName, rgConfig, ruP, ruC);

        activeRG = rgs.resourceGroupGet(activeRgName);
        assertNotNull(activeRG);

        String subscriptionName = "my-subscription";
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicString)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicString)
                .create();

        TopicName myTopic = TopicName.get(topicString);
        rgs.unRegisterTopic(myTopic);
        rgs.registerTopic(activeRgName,myTopic);

        final int NumMessagesToSend = 10;
        int sentNumBytes = 0;
        int sentNumMsgs = 0;
        for (int ix = 0; ix < NumMessagesToSend; ix++) {
            byte[] mesg = String.format("Hi, ix=%s", ix).getBytes();
            producer.send(mesg);
            sentNumBytes += mesg.length;
            sentNumMsgs++;
        }

        this.verifyStats(rgs, topicString, activeRgName, sentNumBytes, sentNumMsgs, 0, 0,
                true, false);

        int recvdNumBytes = 0;
        int recvdNumMsgs = 0;

        Message<byte[]> message;
        while (recvdNumMsgs < sentNumMsgs) {
            message = consumer.receive();
            recvdNumBytes += message.getValue().length;
            recvdNumMsgs++;
        }

        this.verifyStats(rgs,topicString, activeRgName, sentNumBytes, sentNumMsgs, recvdNumBytes, recvdNumMsgs,
                true, true);
    }

    private void verifyStats(ResourceGroupService rgs, String topicString, String rgName,
                             int sentNumBytes, int sentNumMsgs,
                             int recvdNumBytes, int recvdNumMsgs,
                             boolean checkProduce, boolean checkConsume) throws PulsarAdminException {
        BrokerService bs = pulsar.getBrokerService();
        Awaitility.await().untilAsserted(() -> {
            TopicStatsImpl topicStats = bs.getTopicStats().get(topicString);
            assertNotNull(topicStats);
            if (checkProduce) {
                Assert.assertTrue(topicStats.bytesInCounter >= sentNumBytes);
                Assert.assertEquals(sentNumMsgs, topicStats.msgInCounter);
            }
            if (checkConsume) {
                Assert.assertTrue(topicStats.bytesOutCounter >= recvdNumBytes);
                Assert.assertEquals(recvdNumMsgs, topicStats.msgOutCounter);
            }
        });
        if (sentNumMsgs > 0 || recvdNumMsgs > 0) {
            rgs.aggregateResourceGroupLocalUsages();
            BytesAndMessagesCount prodCounts = rgs.getRGUsage(rgName, ResourceGroupMonitoringClass.Publish,
                    ResourceGroupUsageStatsType.Cumulative);
            BytesAndMessagesCount consCounts = rgs.getRGUsage(rgName, ResourceGroupMonitoringClass.Dispatch,
                    ResourceGroupUsageStatsType.Cumulative);

            if (checkProduce) {
                Assert.assertTrue(prodCounts.bytes >= sentNumBytes);
                Assert.assertEquals(sentNumMsgs, prodCounts.messages);
            }
            if (checkConsume) {
                Assert.assertTrue(consCounts.bytes >= recvdNumBytes);
                Assert.assertEquals(recvdNumMsgs, consCounts.messages);
            }
        }
    }
}
