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
package org.apache.pulsar.broker.resourcegroup;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupService.ResourceGroupUsageStatsType;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ResourceGroupUsageAggregationTest extends ProducerConsumerBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        this.prepareData();

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

        ResourceUsageTopicTransportManager transportMgr = new ResourceUsageTopicTransportManager(pulsar);
        this.rgs = new ResourceGroupService(pulsar, TimeUnit.MILLISECONDS, transportMgr, dummyQuotaCalc);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProduceConsumeUsageOnRG() throws Exception {
        testProduceConsumeUsageOnRG(PRODUCE_CONSUME_PERSISTENT_TOPIC);
        testProduceConsumeUsageOnRG(PRODUCE_CONSUME_NON_PERSISTENT_TOPIC);
    }

    private void testProduceConsumeUsageOnRG(String topicString) throws Exception {
        ResourceUsagePublisher ruP = new ResourceUsagePublisher() {
            @Override
            public String getID() { return activeRG.getID(); }
            @Override
            public void fillResourceUsage(ResourceUsage resourceUsage) {
                activeRG.rgFillResourceUsage(resourceUsage);
                numRgFillUsageCallbacks++;
            };
        };

        ResourceUsageConsumer ruC = new ResourceUsageConsumer() {
            @Override
            public String getID() { return activeRG.getID(); }
            @Override
            public void acceptResourceUsage(String broker, ResourceUsage resourceUsage) {
                activeRG.rgResourceUsageListener(broker, resourceUsage);
                numRgUsageListenerCallbacks++;
            }
        };

        rgConfig.setPublishRateInBytes(1500);
        rgConfig.setPublishRateInMsgs(100);
        rgConfig.setDispatchRateInBytes(4000);
        rgConfig.setPublishRateInMsgs(500);
        rgs.resourceGroupCreate(activeRgName, rgConfig, ruP, ruC);

        activeRG = rgs.resourceGroupGet(activeRgName);
        Assert.assertNotEquals(activeRG, null);

        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer = null;

        producer = pulsarClient.newProducer()
                .topic(topicString)
                .create();

        try {
            consumer = pulsarClient.newConsumer()
                    .topic(topicString)
                    .subscriptionName("my-subscription")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();
        } catch (PulsarClientException p) {
            final String errMesg = String.format("Got exception while building consumer: ex={}", p.getMessage());
            Assert.assertTrue(false, errMesg);
        }

        final TopicName myTopic = TopicName.get(topicString);
        final String tenantString = myTopic.getTenant();
        final String nsString = myTopic.getNamespace();
        rgs.registerTenant(activeRgName, tenantString);
        rgs.registerNameSpace(activeRgName, NamespaceName.get(nsString));

        final int NumMessagesToSend = 10;
        int sentNumBytes = 0;
        int sentNumMsgs = 0;
        int recvdNumBytes = 0;
        int recvdNumMsgs = 0;
        for (int ix = 0; ix < NumMessagesToSend; ix++) {
            MessageId prodMesgId = null;
            byte[] mesg;
            try {
                mesg = String.format("Hi, ix={}", ix).getBytes();
                producer.send(mesg);
                sentNumBytes += mesg.length;
                sentNumMsgs++;
            } catch (PulsarClientException p) {
                final String errMesg = String.format("Got exception while sending {}-th time: ex={}", ix, p.getMessage());
                Assert.assertTrue(false, errMesg);
            }
        }
        producer.close();

        this.verfyStats(topicString, activeRgName, sentNumBytes, sentNumMsgs, recvdNumBytes, recvdNumMsgs,
                true, false);

        Message<byte[]> message = null;
        while (recvdNumMsgs < sentNumMsgs) {
            try {
                message = consumer.receive();
                recvdNumBytes += message.getValue().length;
            } catch (PulsarClientException p) {
                final String errMesg = String.format("Got exception in while receiving {}-th mesg at consumer: ex={}",
                        recvdNumMsgs, p.getMessage());
                Assert.assertTrue(false, errMesg);
            }
            // log.info("consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            recvdNumMsgs++;
        }

        this.verfyStats(topicString, activeRgName, sentNumBytes, sentNumMsgs, recvdNumBytes, recvdNumMsgs,
                true, true);

        consumer.close();

        rgs.unRegisterTenant(activeRgName, tenantString);
        rgs.unRegisterNameSpace(activeRgName, NamespaceName.get(nsString));
        rgs.resourceGroupDelete(activeRgName);
    }

    // Verify the app stats with what we see from the broker-service, and the resource-group (which in turn internally
    // derives stats from the broker service)
    // There appears to be a 45-byte message header which is accounted in the stats, additionally to what the
    // application-level sends/receives. Hence, the byte counts are a ">=" check, instead of an equality check.
    private void verfyStats(String topicString, String rgName,
                            int sentNumBytes, int sentNumMsgs,
                            int recvdNumBytes, int recvdNumMsgs,
                            boolean checkProduce, boolean checkConsume)
                                                                throws InterruptedException, PulsarAdminException {
        BrokerService bs = pulsar.getBrokerService();
        Map<String, TopicStatsImpl> topicStatsMap = bs.getTopicStats();
        for (Map.Entry<String, TopicStatsImpl> entry : topicStatsMap.entrySet()) {
            String mapTopicName = entry.getKey();
            if (mapTopicName.equals(topicString)) {
                TopicStatsImpl stats = entry.getValue();
                if (checkProduce) {
                    Assert.assertTrue(stats.bytesInCounter >= sentNumBytes);
                    Assert.assertTrue(stats.msgInCounter == sentNumMsgs);
                }
                if (checkConsume) {
                    Assert.assertTrue(stats.bytesOutCounter >= recvdNumBytes);
                    Assert.assertTrue(stats.msgOutCounter == recvdNumMsgs);
                }

                if (sentNumMsgs > 0 || recvdNumMsgs > 0) {
                    rgs.aggregateResourceGroupLocalUsages();  // hack to ensure aggregator calculation without waiting
                    BytesAndMessagesCount prodCounts = rgs.getRGUsage(rgName, ResourceGroupMonitoringClass.Publish,
                            ResourceGroupUsageStatsType.Cumulative);
                    BytesAndMessagesCount consCounts = rgs.getRGUsage(rgName, ResourceGroupMonitoringClass.Dispatch,
                            ResourceGroupUsageStatsType.Cumulative);

                    // Re-do the getRGUsage.
                    // The counts should be equal, since there wasn't any intervening traffic on TEST_PRODUCE_CONSUME_TOPIC.
                    BytesAndMessagesCount prodCounts1 = rgs.getRGUsage(rgName, ResourceGroupMonitoringClass.Publish,
                            ResourceGroupUsageStatsType.Cumulative);
                    BytesAndMessagesCount consCounts1 = rgs.getRGUsage(rgName, ResourceGroupMonitoringClass.Dispatch,
                            ResourceGroupUsageStatsType.Cumulative);

                    Assert.assertTrue(prodCounts.bytes == prodCounts1.bytes);
                    Assert.assertTrue(prodCounts.messages == prodCounts1.messages);
                    Assert.assertTrue(consCounts.bytes == consCounts1.bytes);
                    Assert.assertTrue(consCounts.messages == consCounts1.messages);

                    if (checkProduce) {
                        Assert.assertTrue(prodCounts.bytes >= sentNumBytes);
                        Assert.assertTrue(prodCounts.messages == sentNumMsgs);
                    }
                    if (checkConsume) {
                        Assert.assertTrue(consCounts.bytes >= recvdNumBytes);
                        Assert.assertTrue(consCounts.messages == recvdNumMsgs);
                    }
                }
            }
        }
    }

    ResourceGroupService rgs;
    ResourceGroup activeRG;
    final org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
            new org.apache.pulsar.common.policies.data.ResourceGroup();
    final String activeRgName = "runProduceConsume";
    int numRgUsageListenerCallbacks = 0;
    int numRgFillUsageCallbacks = 0;

    final String TenantName = "pulsar-test";
    final String NsName = "test";
    final String TenantAndNsName = TenantName + "/" + NsName;
    final String TestProduceConsumeTopicName = "/test/prod-cons-topic";
    final String PRODUCE_CONSUME_PERSISTENT_TOPIC = "persistent://" + TenantAndNsName + TestProduceConsumeTopicName;
    final String PRODUCE_CONSUME_NON_PERSISTENT_TOPIC =
                                                "non-persistent://" + TenantAndNsName + TestProduceConsumeTopicName;
    private static final int PUBLISH_INTERVAL_SECS = 300;

    // Initial set up for transport manager and producer/consumer clusters/tenants/namespaces/topics.
    private void prepareData() throws PulsarAdminException {
        this.conf.setResourceUsageTransportPublishIntervalInSecs(PUBLISH_INTERVAL_SECS);

        this.conf.setAllowAutoTopicCreation(true);

        final String clusterName = "test";
        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
            admin.tenants().createTenant(TenantName,
                    new TenantInfoImpl(Sets.newHashSet("fakeAdminRole"), Sets.newHashSet(clusterName)));
        admin.namespaces().createNamespace(TenantAndNsName);
        admin.namespaces().setNamespaceReplicationClusters(TenantAndNsName, Sets.newHashSet(clusterName));
    }
}
