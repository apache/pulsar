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
import io.prometheus.client.Summary;
import java.util.Collections;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupService.ResourceGroupUsageStatsType;
import org.apache.pulsar.broker.service.BrokerService;
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
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


// The tests implement a set of producer/consumer operations on a set of topics.
// [A thread is started for each producer, and each consumer in the test.]
// The tenants and namespaces in those topics are associated with a set of resource-groups (RGs).
// After sending/receiving all the messages, traffic usage statistics, and Prometheus-metrics
// are verified on the RGs.
public class RGUsageMTAggrWaitForAllMesgsTest extends ProducerConsumerBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        this.prepareForOps();

        ResourceQuotaCalculator dummyQuotaCalc = new ResourceQuotaCalculator() {
            @Override
            public boolean needToReportLocalUsage(long currentBytesUsed, long lastReportedBytes,
                                                  long currentMessagesUsed, long lastReportedMessages,
                                                  long lastReportTimeMSecsSinceEpoch) {
                // Pretend to report every time, just to see the RG-metrics increasing.
                numLocalUsageReports++;
                return true;
            }

            @Override
            public long computeLocalQuota(long confUsage, long myUsage, long[] allUsages) {
                return 0;
            }
        };

        ResourceUsageTopicTransportManager transportMgr = new ResourceUsageTopicTransportManager(pulsar);
        this.rgservice = new ResourceGroupService(pulsar, TimeUnit.SECONDS, transportMgr, dummyQuotaCalc);

        this.prepareRGs();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMTProduceConsumeRGUsage() throws Exception {
        int topicSet= 0;
        for (String[] topicStrings : AllTopicNames) {
            // AllTopicNames has the topic strings in this order:
            //      PersistentTopicNamesSameTenanatAndNsRGs[],
            //      PersistentTopicNamesDifferentTenantAndNsRGs[],
            //      NonPersistentTopicNamesSameTenantAndNsRGs[],
            //      NonPersistentTopicNamesDifferentTenantAndNsRGs[].
            // If the number of RGs is == 1, the "same tenant/NS" topics names and the
            // "different tenant/NS" topic names coincide; so, there's no need to run them distinctly.
            if (NUM_RESOURCE_GROUPS == 1 && (topicSet % 2 == 1)) {
                log.info("NumRGs={}; skipping repetition of test for topic-set {}", NUM_RESOURCE_GROUPS, topicSet);
            } else {
                testProduceConsumeUsageOnRG(topicStrings);
                log.info("Done with topic-set {}", topicSet);
            }
            topicSet++;
        }
        log.info("Done testing with all topics");
    }

    // A class which implements the producer; the main test thread will spawn multiple producers.
    private class produceMessages implements Runnable {
        private int producerId;
        private int numMesgsToProduce;
        private String[] topicStrings;
        private String myProduceTopic;

        private int sentNumBytes = 0;
        private int sentNumMsgs = 0;
        private int numExceptions = 0;

        produceMessages(int prodId, int nMesgs, String[] topics) {
            producerId = prodId;
            numMesgsToProduce = nMesgs;
            topicStrings = topics;
            myProduceTopic = topicStrings[producerId % NUM_TOPICS];
        }

        public int getNumBytesSent() {
            return sentNumBytes;
        }

        public int getNumMessagesSent() {
            return sentNumMsgs;
        }

        public int getNumExceptions() {
            return numExceptions;
        }

        @Override
        public void run() {
            Producer<byte[]> producer = null;

            try {
                // The producer will send messages to a specific topic, since it doesn't make sense for a producer
                // to produce a message with vagueness about the destination topic (neither do Pulsar APIs allow it).
                producer = pulsarClient.newProducer()
                        .topic(myProduceTopic)
                        .create();
            } catch (PulsarClientException p) {
                numExceptions++;
                log.info("Producer={} got exception while building producer: ex={}",
                        producerId, p.getMessage());
            }

            for (int ix = 0; ix < numMesgsToProduce; ix++) {
                byte[] mesg;
                try {
                    mesg = String.format("ProducerId=%d, ix=%d, topic=%s", producerId, ix, myProduceTopic).getBytes();
                    MessageId msgId = producer.send(mesg);
                    sentNumBytes += mesg.length;
                    sentNumMsgs++;
                    log.debug("Producer={}, sent msg-ix={}, msgId={}", producerId, ix, msgId);
                } catch (PulsarClientException p) {
                    numExceptions++;
                    log.info("Producer={} got exception while sending {}-th time: ex={}",
                            producerId, ix, p.getMessage());
                }
            }
            try {
                producer.flush();
                producer.close();
            } catch (PulsarClientException p) {
                numExceptions++;
                log.info("Producer={} got exception while closing producer: ex={}",
                        producerId, p.getMessage());
            }

            log.debug("Producer={} done with topic={}; got {} exceptions", producerId, myProduceTopic, numExceptions);
        }
    }

    // Track the producer object, and the thread using it.
    class producerWithThread {
        produceMessages producer;
        Thread thread;
    }

    // A class which implements the consumer; the main test thread will spawn multiple consumers.
    private class consumeMessages implements Runnable {
        private int consumerId;
        private int numMesgsForThisConsumer;
        private int numTotalMesgsToConsume;
        private SubscriptionType subscriptionType;

        private String[] topicStrings;
        private Consumer<byte[]> consumer = null;

        private int recvTimeoutMilliSecs = 1000;
        private int ackTimeoutMilliSecs = 1100; // has to be more than 1 second
        private int recvdNumBytes = 0;
        private int recvdNumMsgs = 0;
        private int numExceptions = 0;
        private volatile boolean allMessagesReceived = false;
        private volatile boolean consumerIsReady = false;

        consumeMessages(int consId, int nMesgs, int totalMesgs, SubscriptionType subType, String[] topics) {
            consumerId = consId;
            numMesgsForThisConsumer = nMesgs;
            numTotalMesgsToConsume = totalMesgs;
            subscriptionType = subType;
            topicStrings = topics;
        }

        public boolean isConsumerReady() {
            return consumerIsReady;
        }

        public int getNumBytesRecvd() {
            return recvdNumBytes;
        }

        public int getNumMessagesRecvd() {
            return recvdNumMsgs;
        }

        public int getNumExceptions() {
            return numExceptions;
        }

        public void setAllMessagesReceived() { allMessagesReceived = true; }

        public void closeConsumer() {
            try {
                consumerIsReady = false;
                consumer.close();
            } catch (PulsarClientException p) {
                numExceptions++;
                log.error("Consumer={} got exception while closing consumer: ex={}",
                        consumerId, p.getMessage());
            }
        }

        @Override
        public void run() {
            // Create a consumer and subscription, and space for messages, so that they are held for consumption.
            int recvQueueSize = 0;
            String subscriptionString = null;
            switch(subscriptionType) {
                default:
                    numExceptions++;
                    final String errMesg = String.format("Consumer=%d got unexpected subscription type=%s",
                            consumerId, subscriptionType);
                    Assert.assertTrue(false, errMesg);
                    break;
                case Shared:
                    recvQueueSize = numTotalMesgsToConsume;
                    subscriptionString = "my-subscription";
                    break;
                case Exclusive:
                    recvQueueSize = numMesgsForThisConsumer;
                    subscriptionString = "my-subscription-" + consumerId;
                    break;
            }

            try {
                // The consumer will try to get a message from any of the topics, since Pulsar allows a consumer to
                // be subscribed to multiple topics.
                consumer = pulsarClient.newConsumer()
                        .topic(topicStrings)
                        .subscriptionName(subscriptionString)
                        .subscriptionType(subscriptionType)
                        .receiverQueueSize(recvQueueSize)
                        .ackTimeout(ackTimeoutMilliSecs, TimeUnit.MILLISECONDS)
                        .subscribe();
            } catch (PulsarClientException p) {
                numExceptions++;
                log.error("Consumer={} got exception while building consumer: ex={}",
                        consumerId, p.getMessage());
            }

            Message<byte[]> message = null;
            consumerIsReady = true;
            while (consumerIsReady && !allMessagesReceived) {
                log.debug("Consumer={} waiting for mesgnum={}", consumerId, recvdNumMsgs);
                try {
                    message = consumer.receive(recvTimeoutMilliSecs, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        consumer.acknowledgeAsync(message);
                        String mesg = String.format("Consumer=%d recvd %d-th mesg; id=%s, data=%s",
                                consumerId, recvdNumMsgs, message.getMessageId(), new String(message.getData()));
                        log.debug(mesg);
                        recvdNumBytes += message.getValue().length;
                        recvdNumMsgs++;
                    }
                } catch (PulsarClientException p) {
                    numExceptions++;
                    log.error("Consumer={} got exception in while receiving {}-th mesg at consumer: ex={}",
                            consumerId, recvdNumMsgs, p.getMessage());
                }
            }

            log.debug("Consumer={} done; got {} exceptions", consumerId, numExceptions);
        }
    }

    // Track the consumer object, and the thread using it.
    class consumerWithThread {
        consumeMessages consumer;
        Thread thread;
    }

    // Given a topic, get the tenant RG-name
    private String TopicToTenantRGName (TopicName topicName) {
        // Under the current topic naming scheme, the tenant-rg name is just the tenant part of the topic.
        String tenant = topicName.getTenant();
        return tenant;
    }

    // Given a topic, get the namespace RG-name
    private String TopicToNamespaceRGName (TopicName topicName) {
        // Under the current topic naming  scheme, the namespace-rg name is just the namespace part of the topic.
        String nameSpace = topicName.getNamespacePortion();
        return nameSpace;
    }

    // Return true if the tenant-RG == namespace-RG in the topics given, false otherwise.
    // If some are equal, and others unequal, throw, becasue thisis unexpected in this UT at the moment.
    private boolean tenantRGEqualsNamespaceRG(String[] topicStrings) throws PulsarClientException {
        int numEqualRGs = 0;
        int numUnEqualRGs = 0;
        int numTopics = topicStrings.length;
        for (String topicStr : topicStrings) {
            TopicName topic = TopicName.get(topicStr);
            String tenantRG = TopicToTenantRGName(topic);
            String namespaceRG = TopicToNamespaceRGName(topic);
            if (tenantRG.compareTo(namespaceRG) == 0) {
                numEqualRGs++;
            } else {
                numUnEqualRGs++;
            }
        }
        if ((numEqualRGs + numUnEqualRGs != numTopics) || (numEqualRGs > 0 && numUnEqualRGs > 0)) {
            String errMesg = String.format("Found {} topics with equal RGs and {} with unequal, on {} topics",
                    numEqualRGs, numUnEqualRGs, numTopics);
            throw new PulsarClientException(errMesg);
        } else if (numEqualRGs == numTopics) {
            return true;
        }
        return false;
    }

    private void registerTenantsAndNamespaces(String[] topicStrings) throws Exception {
        for (String topicStr : topicStrings) {
            final TopicName topic = TopicName.get(topicStr);
            final String tenantRG = TopicToTenantRGName(topic);
            final String namespaceRG = TopicToNamespaceRGName(topic);
            final NamespaceName ns = topic.getNamespaceObject();

            // The tenant name and namespace name parts of the topic are the same as their corresponding RG-names.
            // Hence, the arguments to register look a little odd.
            if (!registeredTenants.contains(tenantRG)) {
                this.rgservice.registerTenant(tenantRG, tenantRG);
                registeredTenants.add(tenantRG);
            }
            if (!registeredNamespaces.contains(namespaceRG)) {
                this.rgservice.registerNameSpace(namespaceRG, ns);
                registeredNamespaces.add(namespaceRG);
            }
        }
    }

    private void unRegisterTenantsAndNamespaces(String[] topicStrings) throws Exception {
        for (String topicStr : topicStrings) {
            final TopicName topic = TopicName.get(topicStr);
            final String tenantRG = TopicToTenantRGName(topic);
            final String namespaceRG = TopicToNamespaceRGName(topic);
            final String tenantAndNamespace = topic.getNamespace();;

            // The tenant name and namespace name parts of the topic are the same as their corresponding RG-names.
            // Hence, the arguments to unRegister look a little odd.
            if (registeredTenants.contains(tenantRG)) {
                this.rgservice.unRegisterTenant(tenantRG, tenantRG);
                registeredTenants.remove(tenantRG);
            }
            if (registeredNamespaces.contains(namespaceRG)) {
                this.rgservice.unRegisterNameSpace(namespaceRG, NamespaceName.get(tenantAndNamespace));
                registeredNamespaces.remove(namespaceRG);
            }
        }
    }

    // Produce/consume messages on the given topics, and verify that the resource-group stats are updated.
    private void testProduceConsumeUsageOnRG(String[] topicStrings) throws Exception {
        createRGs();
        // creating the topics results in exposing a regression.
        // It can be put back after https://github.com/apache/pulsar/issues/11289 is fixed.
        // createTopics(topicStrings);
        registerTenantsAndNamespaces(topicStrings);

        final int TotalExpectedMessagesToSend = NUM_TOTAL_MESSAGES;
        final int TotalExpectedMessagesToReceive = TotalExpectedMessagesToSend;

        final SubscriptionType consumeSubscriptionType = SubscriptionType.Shared;  // Shared, or Exclusive

        producerWithThread prodThr[] = new producerWithThread[NUM_PRODUCERS];
        consumerWithThread consThr[] = new consumerWithThread[NUM_CONSUMERS];
        int sentNumBytes = 0;
        int sentNumMsgs = 0;
        int numProducerExceptions = 0;

//        // Verify that stats on the topic are all-zero in the beginning.
//        this.verfyRGProdConsStats(topicStrings, 0, 0, 0, 0,true, true);

        // Fork some consumers to receive the messages.
        for (int ix = 0; ix < NUM_CONSUMERS; ix++) {
            consThr[ix] = new consumerWithThread();
            consumeMessages cm = new consumeMessages(ix, NUM_MESSAGES_PER_CONSUMER, TotalExpectedMessagesToReceive,
                                                     consumeSubscriptionType, topicStrings);
            Thread thr = new Thread(cm);
            thr.start();
            consThr[ix].consumer = cm;
            consThr[ix].thread = thr;
        }

        // Wait for all consumers to be ready, before forking producers, so we don't lose messages.
        int numReadyConsumers;
        do {
            Thread.sleep(500);
            numReadyConsumers = 0;
            for (int ix = 0; ix < NUM_CONSUMERS; ix++) {
                if (consThr[ix].consumer.isConsumerReady()) {
                    numReadyConsumers++;
                }
            }
            log.debug("{} consumers are not yet ready", NUM_CONSUMERS - numReadyConsumers);
        } while (numReadyConsumers < NUM_CONSUMERS);

        // Fork some producers to send the messages.
        for (int ix = 0; ix < NUM_PRODUCERS; ix++) {
            prodThr[ix] = new producerWithThread();
            produceMessages pm = new produceMessages(ix, NUM_MESSAGES_PER_PRODUCER, topicStrings);
            Thread thr = new Thread(pm);
            thr.start();
            prodThr[ix].producer = pm;
            prodThr[ix].thread = thr;
        }

        // Wait for the producers to complete.
        int sentMsgs, sentBytes;
        for (int ix = 0; ix < NUM_PRODUCERS; ix++) {
            prodThr[ix].thread.join();
            sentBytes = prodThr[ix].producer.getNumBytesSent();
            sentMsgs = prodThr[ix].producer.getNumMessagesSent();
            numProducerExceptions += prodThr[ix].producer.getNumExceptions();

            log.debug("Producer={} sent {} mesgs and {} bytes", ix, sentMsgs, sentBytes);
            sentNumBytes += sentBytes;
            sentNumMsgs += sentMsgs;
        }
        Assert.assertEquals(sentNumMsgs, TotalExpectedMessagesToSend);
        Assert.assertEquals(numProducerExceptions, 0);

        int recvdNumBytes = 0;
        int recvdNumMsgs;
        int numConsumerExceptions = 0;

        // Wait for the consumers to receive all the messages.
        do {
            Thread.sleep(2000);
            recvdNumMsgs = 0;
            int consNumMesgsRecvd;
            for (int ix = 0; ix < NUM_CONSUMERS; ix++) {
                consNumMesgsRecvd = consThr[ix].consumer.getNumMessagesRecvd();
                recvdNumMsgs += consNumMesgsRecvd;
                log.debug("consumer={} received {} messages (current total {}, expected {})",
                        ix, consNumMesgsRecvd, recvdNumMsgs, TotalExpectedMessagesToReceive);
            }
        } while (recvdNumMsgs < TotalExpectedMessagesToReceive);

        // Tell the consumers that all expected messages have been received (but don't close them yet).
        for (int ix = 0; ix < NUM_CONSUMERS; ix++) {
            consThr[ix].consumer.setAllMessagesReceived();
            log.debug("consumer={} told to stop", ix);
        }

        boolean[] joinedConsumers = new boolean[NUM_CONSUMERS];
        for (boolean b : joinedConsumers) {
            b = false;
        }

        recvdNumMsgs = recvdNumBytes = 0;
        int numConsumersDone = 0;
        int recvdMsgs, recvdBytes;
        while (numConsumersDone < NUM_CONSUMERS) {
            for (int ix = 0; ix < NUM_CONSUMERS; ix++) {
                if (joinedConsumers[ix] == false)  {
                    recvdBytes = consThr[ix].consumer.getNumBytesRecvd();
                    recvdMsgs = consThr[ix].consumer.getNumMessagesRecvd();
                    numConsumerExceptions += consThr[ix].consumer.getNumExceptions();
                    log.debug("Consumer={} received {} mesgs and {} bytes", ix, recvdMsgs, recvdBytes);

                    consThr[ix].thread.join();
                    joinedConsumers[ix] = true;
                    log.debug("Joined consumer={}", ix);

                    recvdNumBytes += recvdBytes;
                    recvdNumMsgs += recvdMsgs;
                    numConsumersDone++;
                }
            }
        }

        // Close the consumers.
        for (int ix = 0; ix < NUM_CONSUMERS; ix++) {
            consThr[ix].consumer.closeConsumer();
        }

        Assert.assertEquals(recvdNumMsgs, TotalExpectedMessagesToReceive);
        Assert.assertEquals(numConsumerExceptions, 0);

//        // Verify the producer side stats (msgInCounter/bytesInCounter) only.
//        this.verfyRGProdConsStats(topicStrings, sentNumBytes, sentNumMsgs, 0, 0,true, false);
//        this.verifyRGMetrics(topicStrings, sentNumBytes, sentNumMsgs, recvdNumBytes, recvdNumMsgs, true, false);

        // Verify producer and consumer side stats.
        this.verfyRGProdConsStats(topicStrings, sentNumBytes, sentNumMsgs, recvdNumBytes, recvdNumMsgs, true, true);

        // Verify the metrics corresponding to the operations in this test.
        this.verifyRGMetrics(topicStrings, sentNumBytes, sentNumMsgs, recvdNumBytes, recvdNumMsgs, true, true);

        unRegisterTenantsAndNamespaces(topicStrings);
        // destroyTopics can be called after createTopics() is added back
        // (see comment above regarding https://github.com/apache/pulsar/issues/11289).
        // destroyTopics(topicStrings);
        destroyRGs();
    }

    // Verify the app stats with what we see from the broker-service, and the resource-group (which in turn internally
    // derives stats from the broker service)
    private void verfyRGProdConsStats(String[] topicStrings,
                            int sentNumBytes, int sentNumMsgs,
                            int recvdNumBytes, int recvdNumMsgs,
                            boolean checkProduce, boolean checkConsume)  throws Exception {

        boolean tenantRGEqualsNsRG = tenantRGEqualsNamespaceRG(topicStrings);
        BrokerService bs = pulsar.getBrokerService();
        Map<String, TopicStatsImpl> topicStatsMap = bs.getTopicStats();

        log.debug("verfyProdConsStats: topicStatsMap has {} entries", topicStatsMap.size());

        // Pulsar runtime adds some additional bytes in the exchanges: a 45-byte per-message
        // metadata of some kind, plus more as the number of messages increases.
        // Hence the ">=" assertion with ExpectedNumBytesSent/Received in the following checks.
        final int ExpectedNumBytesSent = sentNumBytes + PER_MESSAGE_METADATA_OHEAD * sentNumMsgs;
        final int ExpectedNumBytesReceived = recvdNumBytes + PER_MESSAGE_METADATA_OHEAD * recvdNumMsgs;

        long totalOutMessages = 0, totalOutBytes = 0;
        long totalInMessages = 0, totalInBytes = 0;
        BytesAndMessagesCount totalTenantRGProdCounts = new BytesAndMessagesCount();
        BytesAndMessagesCount totalTenantRGConsCounts = new BytesAndMessagesCount();
        BytesAndMessagesCount totalNsRGProdCounts = new BytesAndMessagesCount();
        BytesAndMessagesCount totalNsRGConsCounts = new BytesAndMessagesCount();
        BytesAndMessagesCount prodCounts, consCounts;

        // If the tenant and NS are on different RGs, the bytes/messages get counted once on the
        // tenant RG, and again on the namespace RG. This double-counting is avoided if tenant-RG == NS-RG.
        // This is a known (and discussed) artifact in the implementation.
        // 'ScaleFactor' is a way to incorporate that effect in the verification.
        final int ScaleFactor = 1; // tenantRGEqualsNsRG ? 1 : 2;

        // Following sleep is to get the broker-service to gather stats on the topics.
        // [There appears to be some asynchrony there.]
        // Thread.sleep(5 * 1000);

        // Since the following walk is on topics, keep track of the RGs for which we have already gathered stats,
        // so that we do not double-accumulate stats if multiple topics refer to the same RG.
        HashSet<String> RGsWithPublishStatsGathered = new HashSet<>();
        HashSet<String> RGsWithDispatchStatsGathered = new HashSet<>();

        // Hack to ensure aggregator calculation without waiting for a period of aggregation.
        // [aggregateResourceGroupLocalUsages() is idempotent when there's no fresh traffic flowing.]
        this.rgservice.aggregateResourceGroupLocalUsages();

        for (Map.Entry<String, TopicStatsImpl> entry : topicStatsMap.entrySet()) {
            String mapTopicName = entry.getKey();
            if (Arrays.asList(topicStrings).contains(mapTopicName)) {
                TopicStats stats = entry.getValue();
                totalInMessages += stats.getMsgInCounter();
                totalInBytes += stats.getBytesInCounter();
                totalOutMessages += stats.getMsgOutCounter();
                totalOutBytes += stats.getBytesOutCounter();

                // Assuming that broker-service stats-gathering is doing its job,
                // we should see some produced mesgs on every topic.
                if (totalInMessages == 0) {
                    log.warn("verfyProdConsStats: found no produced mesgs (msgInCounter) on topic {}", mapTopicName);
                }

                if (sentNumMsgs > 0 || recvdNumMsgs > 0) {
                    TopicName topic = TopicName.get(mapTopicName);

                    final String tenantRGName = TopicToTenantRGName(topic);
                    if (!RGsWithPublishStatsGathered.contains(tenantRGName)) {
                        prodCounts = this.rgservice.getRGUsage(tenantRGName, ResourceGroupMonitoringClass.Publish,
                                                               getCumulativeUsageStats);
                        totalTenantRGProdCounts = ResourceGroup.accumulateBMCount(totalTenantRGProdCounts, prodCounts);
                        RGsWithPublishStatsGathered.add(tenantRGName);
                    }
                    if (!RGsWithDispatchStatsGathered.contains(tenantRGName)) {
                        consCounts = this.rgservice.getRGUsage(tenantRGName, ResourceGroupMonitoringClass.Dispatch,
                                                               getCumulativeUsageStats);
                        totalTenantRGConsCounts = ResourceGroup.accumulateBMCount(totalTenantRGConsCounts, consCounts);
                        RGsWithDispatchStatsGathered.add(tenantRGName);
                    }

                    final String nsRGName = TopicToNamespaceRGName(topic);
                    // If tenantRGName == nsRGName, the RG-infra will avoid double counting.
                    // We will do the same here, to get the expected stats.
                    if (tenantRGName.compareTo(nsRGName) != 0) {
                        if (!RGsWithPublishStatsGathered.contains(nsRGName)) {
                            prodCounts = this.rgservice.getRGUsage(nsRGName, ResourceGroupMonitoringClass.Publish,
                                                                   getCumulativeUsageStats);
                            totalNsRGProdCounts = ResourceGroup.accumulateBMCount(totalNsRGProdCounts, prodCounts);
                            RGsWithPublishStatsGathered.add(nsRGName);
                        }
                        if (!RGsWithDispatchStatsGathered.contains(nsRGName)) {
                            consCounts = this.rgservice.getRGUsage(nsRGName, ResourceGroupMonitoringClass.Dispatch,
                                                                   getCumulativeUsageStats);
                            totalNsRGConsCounts = ResourceGroup.accumulateBMCount(totalNsRGConsCounts, consCounts);
                            RGsWithDispatchStatsGathered.add(nsRGName);
                        }
                    }
                }
            }
        }

        // Check that the accumulated totals tally up.
        if (checkConsume && checkProduce) {
            Assert.assertEquals(totalOutMessages, totalInMessages);
            Assert.assertEquals(totalOutBytes, totalInBytes);
        }

        if (checkProduce) {
            Assert.assertEquals(totalInMessages, sentNumMsgs);
            Assert.assertTrue(totalInBytes >= ExpectedNumBytesSent);
        }

        if (checkConsume) {
            Assert.assertEquals(totalOutMessages, recvdNumMsgs);
            Assert.assertTrue(totalOutBytes >= ExpectedNumBytesReceived);
        }

        if (checkProduce) {
            prodCounts = ResourceGroup.accumulateBMCount(totalTenantRGProdCounts, totalNsRGProdCounts);
            Assert.assertEquals(prodCounts.messages, sentNumMsgs);
            Assert.assertTrue(prodCounts.bytes >= ExpectedNumBytesSent);
        }

        if (checkConsume) {
            consCounts = ResourceGroup.accumulateBMCount(totalTenantRGConsCounts, totalNsRGConsCounts);
            Assert.assertEquals(consCounts.messages, recvdNumMsgs);
            Assert.assertTrue(consCounts.bytes >= ExpectedNumBytesReceived);
        }
    }

    // Check the metrics for the RGs involved
    private void verifyRGMetrics(String[] topicStrings,
                                 int sentNumBytes, int sentNumMsgs,
                                 int recvdNumBytes, int recvdNumMsgs,
                                 boolean checkProduce, boolean checkConsume) throws Exception {

        boolean tenantRGEqualsNsRG = tenantRGEqualsNamespaceRG(topicStrings);
        final int ExpectedNumBytesSent = sentNumBytes + PER_MESSAGE_METADATA_OHEAD * sentNumMsgs;
        final int ExpectedNumBytesReceived = recvdNumBytes + PER_MESSAGE_METADATA_OHEAD * recvdNumMsgs;
        long totalTenantRegisters = 0;
        long totalTenantUnRegisters = 0;
        long totalNamespaceRegisters = 0;
        long totalNamespaceUnRegisters = 0;
        long[] totalQuotaBytes = new long[ResourceGroupMonitoringClass.values().length];
        long[] totalQuotaMessages = new long[ResourceGroupMonitoringClass.values().length];
        long[] totalUsedBytes = new long[ResourceGroupMonitoringClass.values().length];
        long[] totalUsedMessages = new long[ResourceGroupMonitoringClass.values().length];
        long[] totalUsageReportCounts = new long[ResourceGroupMonitoringClass.values().length];
        long totalUpdates = 0;

        // Hack to ensure aggregator calculation without waiting for a period of aggregation.
        // [aggregateResourceGroupLocalUsages() is idempotent when there's no new traffic flowing.]
        this.rgservice.aggregateResourceGroupLocalUsages();

        for (String rgName : RGNames) {
            for (ResourceGroupMonitoringClass mc : ResourceGroupMonitoringClass.values()) {
                String mcName = mc.name();
                int mcIndex = mc.ordinal();
                double quotaBytes = ResourceGroupService.getRgQuotaByteCount(rgName, mcName);
                totalQuotaBytes[mcIndex] += quotaBytes;
                double quotaMesgs = ResourceGroupService.getRgQuotaMessageCount(rgName, mcName);
                totalQuotaMessages[mcIndex] += quotaMesgs;
                double usedBytes = ResourceGroupService.getRgLocalUsageByteCount(rgName, mcName);
                totalUsedBytes[mcIndex] += usedBytes;
                double usedMesgs = ResourceGroupService.getRgLocalUsageMessageCount(rgName, mcName);
                totalUsedMessages[mcIndex] += usedMesgs;

                double usageReportedCount = ResourceGroup.getRgUsageReportedCount(rgName, mcName);
                totalUsageReportCounts[mcIndex] += usageReportedCount;
            }

            totalTenantRegisters += ResourceGroupService.getRgTenantRegistersCount(rgName);
            totalTenantUnRegisters += ResourceGroupService.getRgTenantUnRegistersCount(rgName);
            totalNamespaceRegisters += ResourceGroupService.getRgNamespaceRegistersCount(rgName);
            totalNamespaceUnRegisters += ResourceGroupService.getRgNamespaceUnRegistersCount(rgName);;
            totalUpdates += ResourceGroupService.getRgUpdatesCount(rgName);
        }
        log.info("totalTenantRegisters={}, totalTenantUnRegisters={}, " +
                        "totalNamespaceRegisters={}, totalNamespaceUnRegisters={}",
                totalTenantRegisters, totalTenantUnRegisters, totalNamespaceRegisters, totalNamespaceUnRegisters);

        // On each run, there will be 'NumRGs' registrations
        Assert.assertEquals(totalTenantRegisters - residualTenantRegs, NUM_RESOURCE_GROUPS);
        Assert.assertEquals(totalNamespaceRegisters - residualNamespaceRegs, NUM_RESOURCE_GROUPS);

        // The unregisters will lag the registers by one round (because verifyRGMetrics() is called
        // prior to unregister). In other words, their numbers will equal the residuals for the registers.
        Assert.assertEquals(totalTenantUnRegisters, residualTenantRegs);
        Assert.assertEquals(totalNamespaceUnRegisters, residualNamespaceRegs);

        // Update residuals for next test run.
        residualTenantRegs = totalTenantRegisters;
        residualNamespaceRegs = totalNamespaceRegisters;

        for (ResourceGroupMonitoringClass mc : ResourceGroupMonitoringClass.values()) {
            int mcIdx = mc.ordinal();
            log.info("mc={}: totalQuotaBytes={}, totalQuotaMessages={}, " +
                            " totalUsedBytes={}, totalUsedMessages={}" +
                            " totalUsageReports={}",
                    mc.name(), totalQuotaBytes[mcIdx], totalQuotaMessages[mcIdx],
                    totalUsedBytes[mcIdx], totalUsedMessages[mcIdx], totalUsageReportCounts[mcIdx]);
            // On each run, the bytes/messages are monotone incremented in Prometheus metrics.
            // So, we take the residuals into account when comparing against the expected.
            if (checkProduce && mc == ResourceGroupMonitoringClass.Publish) {
                Assert.assertEquals(totalUsedMessages[mcIdx] - residualSentNumMessages,
                                                                                sentNumMsgs);
                Assert.assertTrue(totalUsedBytes[mcIdx] - residualSentNumBytes
                                                                                >= ExpectedNumBytesSent);
            } else if (checkConsume && mc == ResourceGroupMonitoringClass.Dispatch) {
                Assert.assertEquals(totalUsedMessages[mcIdx] - residualRecvdNumMessages,
                                                                                recvdNumMsgs);
                Assert.assertTrue(totalUsedBytes[mcIdx] - residualRecvdNumBytes
                                                                            >= ExpectedNumBytesReceived);
            }

            long perClassUsageReports = numLocalUsageReports / ResourceGroupMonitoringClass.values().length;
            Assert.assertEquals(totalUsageReportCounts[mcIdx], perClassUsageReports);
        }

        // Update the residuals for next round of tests.
        residualSentNumBytes += sentNumBytes;
        residualSentNumMessages += sentNumMsgs;
        residualRecvdNumBytes += recvdNumBytes;
        residualRecvdNumMessages += recvdNumMsgs;

        Assert.assertEquals(totalUpdates, 0);  // currently, we don't update the RGs in this UT

        // Basic check that latency metrics are doing some work.
        Summary.Child.Value usageAggrLatency = ResourceGroupService.getRgUsageAggregationLatency();
        Assert.assertNotEquals(usageAggrLatency.count, 0);
        Assert.assertNotEquals(usageAggrLatency.sum, 0);
        double fiftiethPercentileValue = usageAggrLatency.quantiles.get(0.5);
        Assert.assertNotEquals(fiftiethPercentileValue, 0);
        double ninetethPercentileValue = usageAggrLatency.quantiles.get(0.9);
        Assert.assertNotEquals(ninetethPercentileValue, 0);

        Summary.Child.Value quotaCalcLatency = ResourceGroupService.getRgQuotaCalculationTime();
        Assert.assertNotEquals(quotaCalcLatency.count, 0);
        Assert.assertNotEquals(quotaCalcLatency.sum, 0);
        fiftiethPercentileValue = quotaCalcLatency.quantiles.get(0.5);
        Assert.assertNotEquals(fiftiethPercentileValue, 0);
        ninetethPercentileValue = quotaCalcLatency.quantiles.get(0.9);
        Assert.assertNotEquals(ninetethPercentileValue, 0);
    }

    private static final Logger log = LoggerFactory.getLogger(RGUsageMTAggrWaitForAllMesgsTest.class);

    // Empirically, there appears to be a 45-byte overhead for metadata, imposed by Pulsar runtime.
    private static final int PER_MESSAGE_METADATA_OHEAD = 45;

    private static final int PUBLISH_INTERVAL_SECS = 10;
    private static final int NUM_PRODUCERS = 4;
    private static final int NUM_CONSUMERS = 4;
    private static final int NUM_MESSAGES_PER_PRODUCER = 100;
    private static final int NUM_TOPICS = 8;  // Set == NumProducers, so each producer can send on its own topic
    private static final int NUM_RESOURCE_GROUPS = 4; // arbitrarily, half of NumTopics, so 2 topics map to each RG
    private static final int NUM_TOTAL_MESSAGES = NUM_MESSAGES_PER_PRODUCER * NUM_PRODUCERS;
    private static  final int NUM_MESSAGES_PER_CONSUMER = NUM_TOTAL_MESSAGES / NUM_CONSUMERS;
    private final org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
                                                            new org.apache.pulsar.common.policies.data.ResourceGroup();
    private ResourceGroupService rgservice;
    private ResourceGroup[] resGroups = new ResourceGroup[NUM_RESOURCE_GROUPS];

    private final String clusterName = "test";
    private final String BaseRGName = "rg-";
    private final String BaseTestTopicName = "rgusage-topic-";

    private String[] RGNames = new String[NUM_RESOURCE_GROUPS];

    // The number of times we pretend to have not suppressed sending a local usage report.
    private long numLocalUsageReports;

    // Combinations of tenant and namespace required to test RG use cases when both NS and tenant are under the control
    // of the same RG, vs. cases where they are under the control of distinct RGs.
    // [This is required to test the special case of "tenant and NS refer to the same RG", because in that case
    // we don't double-count the usage.]
    // Same-order mapping: e.g., rg-0/rg-0 (for 0th entry)
    private String[] TenantAndNsNameSameOrder = new String[NUM_RESOURCE_GROUPS];

    // Opposite order mapping: e.g., rg-0/rg-49 (for 0th entry with 50 RGs)
    private String[] TenantAndNsNameOppositeOrder = new String[NUM_RESOURCE_GROUPS];

    // Similar to above (same and opposite order) for topics.
    // E.g., rg-0/rg-0/rgusage-topic0 for 0-th topic in "same order"
    // and rg-0/rg-49/rgusage-topic0 for 0-th topic in "opposite order", with 50 RGs
    private String[] TopicNamesSameTenantAndNsRGs = new String[NUM_TOPICS];
    private String[] TopicNamesDifferentTenantAndNsRGs = new String[NUM_TOPICS];

    // Persistent and non-persistent topic strings with the above names.
    private String[] PersistentTopicNamesSameTenanatAndNsRGs = new String[NUM_TOPICS];
    private String[] PersistentTopicNamesDifferentTenantAndNsRGs = new String[NUM_TOPICS];
    private String[] NonPersistentTopicNamesSameTenantAndNsRGs = new String[NUM_TOPICS];
    private String[] NonPersistentTopicNamesDifferentTenantAndNsRGs = new String[NUM_TOPICS];

    private List<String[]> AllTopicNames = Arrays.asList(
            PersistentTopicNamesSameTenanatAndNsRGs,
            PersistentTopicNamesDifferentTenantAndNsRGs,
            NonPersistentTopicNamesSameTenantAndNsRGs,
            NonPersistentTopicNamesDifferentTenantAndNsRGs
    );



    // We don't periodically report to a remote broker in this test. So, we will use cumulative stats.
    private final ResourceGroupUsageStatsType getCumulativeUsageStats = ResourceGroupUsageStatsType.Cumulative;

    // Keep track of the namespaces that were created, so we don't dup and get exceptions
    HashSet<String> createdNamespaces = new HashSet<>();

    // Keep track of the topics that were created, so we don't dup and get exceptions
    HashSet<String> createdTopics = new HashSet<>();

    // Keep track of the tenants that have been registered to their RGs, so we don't dup and get exceptions
    HashSet<String> registeredTenants = new HashSet<>();

    // Keep track of the namespaces that have been registered to their RGs, so we don't dup and get exceptions
    HashSet<String> registeredNamespaces = new HashSet<>();

    // Prometheus stats are monotonically increasing numbers.
    // On each run, the resource-group metrics are incremented in Prometheus.
    // So, we keep some residuals to help isolate/verify "this run's" values.
    long residualTenantRegs;
    long residualNamespaceRegs;
    long residualSentNumBytes;
    long residualSentNumMessages;
    long residualRecvdNumBytes;
    long residualRecvdNumMessages;

    // Create the topics provided
    private void createTopics(String[] topics) {
        BrokerService bs = this.pulsar.getBrokerService();
        for (String topic : topics) {
            if (!createdTopics.contains(topic)) {
                bs.getOrCreateTopic(topic);
                createdTopics.add(topic);
            }
        }
    }

    // Destroy the topics provided
    private void destroyTopics(String[] topics) {
        BrokerService bs = this.pulsar.getBrokerService();
        for (String topic : topics) {
            if (!createdTopics.contains(topic)) {
                bs.deleteTopic(topic, true);
                createdTopics.remove(topic);
            }
        }
    }

    // Create all the RGs named in RGNames[]
    private void createRGs() throws Exception {
        for (String rgname : RGNames) {
            this.rgservice.resourceGroupCreate(rgname, rgConfig);
        }
    }

    // Destroy all the RGs named in RGNames[]
    private void destroyRGs() throws Exception {
        for (String rgname : RGNames) {
            this.rgservice.resourceGroupDelete(rgname);
        }
    }

    // Initial set up for transport manager and cluster creation.
    private void prepareForOps() throws PulsarAdminException {
        this.conf.setResourceUsageTransportPublishIntervalInSecs(PUBLISH_INTERVAL_SECS);
        this.conf.setAllowAutoTopicCreation(true);
        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
    }

    // Set up of RG/tenant/namespaces/topic names, and checking of the test parameters.
    private void prepareRGs() throws Exception {
        // Check for a few invariants which allow easier mapping of structures in the test.

        // Ensure that the number of consumers is a multiple of the number of producers.
        Assert.assertTrue(NUM_CONSUMERS >= NUM_PRODUCERS && NUM_CONSUMERS % NUM_PRODUCERS == 0);

        // Number of messages is a multiple of the number of topics.
        Assert.assertEquals(NUM_TOTAL_MESSAGES % NUM_TOPICS,  0);

        // Ensure that the number of topics is a multiple of the number of RGs.
        Assert.assertEquals(NUM_TOPICS % NUM_RESOURCE_GROUPS,  0);

        // Ensure that the messages-per-consumer is an integral multiple of the number of consumers.
        final int NumConsumerMessages = NUM_MESSAGES_PER_CONSUMER * NUM_CONSUMERS;
        final int NumProducerMessages = NUM_MESSAGES_PER_PRODUCER * NUM_PRODUCERS;
        Assert.assertTrue(NUM_MESSAGES_PER_CONSUMER > 0 && NumConsumerMessages == NumProducerMessages);

        rgConfig.setPublishRateInBytes(1500);
        rgConfig.setPublishRateInMsgs(100);
        rgConfig.setDispatchRateInBytes(4000);
        rgConfig.setDispatchRateInMsgs(500);

        // Set up the RG names; creation of RGs will be done elsewhere.
        for (int ix = 0; ix < NUM_RESOURCE_GROUPS; ix++) {
            RGNames[ix] = BaseRGName + ix;
        }

        // Create all the tenants
        final TenantInfo configInfo =
                new TenantInfoImpl(Sets.newHashSet("fakeAdminRole"), Sets.newHashSet(clusterName));
        for (int ix = 0; ix < NUM_RESOURCE_GROUPS; ix++) {
            admin.tenants().createTenant(RGNames[ix], configInfo);
        }

        // Set up the tenant-and-nsname mapping strings, for same and opposite order of RGs.
        for (int ix = 0; ix < NUM_RESOURCE_GROUPS; ix++) {
            TenantAndNsNameSameOrder[ix] = RGNames[ix] + "/" + RGNames[ix];
            TenantAndNsNameOppositeOrder[ix] = RGNames[ix] + "/" + RGNames[NUM_RESOURCE_GROUPS - (ix+1)];
        }

        // Create all the namespaces
        for (int ix = 0; ix < NUM_RESOURCE_GROUPS; ix++) {
            if (!createdNamespaces.contains(TenantAndNsNameSameOrder[ix])) {
                admin.namespaces().createNamespace(TenantAndNsNameSameOrder[ix]);
                admin.namespaces().setNamespaceReplicationClusters(
                        TenantAndNsNameSameOrder[ix], Sets.newHashSet(clusterName));
                createdNamespaces.add(TenantAndNsNameSameOrder[ix]);
            }

            if (!createdNamespaces.contains(TenantAndNsNameOppositeOrder[ix])) {
                admin.namespaces().createNamespace(TenantAndNsNameOppositeOrder[ix]);
                admin.namespaces().setNamespaceReplicationClusters(
                        TenantAndNsNameOppositeOrder[ix], Sets.newHashSet(clusterName));
                createdNamespaces.add(TenantAndNsNameOppositeOrder[ix]);
            }
        }

        // Create all the topic name strings
        for (int ix = 0; ix < NUM_TOPICS; ix++) {
            TopicNamesSameTenantAndNsRGs[ix] =
                    TenantAndNsNameSameOrder[ix % NUM_RESOURCE_GROUPS] + "/" + BaseTestTopicName + ix;
            TopicNamesDifferentTenantAndNsRGs[ix] =
                    TenantAndNsNameOppositeOrder[ix % NUM_RESOURCE_GROUPS] + "/" + BaseTestTopicName + ix;
        }

        // Create all the persistent and non-persistent topic strings
        for (int ix = 0; ix < NUM_TOPICS; ix++) {
            PersistentTopicNamesSameTenanatAndNsRGs[ix] =
                    "persistent://" + TopicNamesSameTenantAndNsRGs[ix];
            PersistentTopicNamesDifferentTenantAndNsRGs[ix] =
                    "persistent://" + TopicNamesDifferentTenantAndNsRGs[ix];

            NonPersistentTopicNamesSameTenantAndNsRGs[ix] =
                    "non-persistent://" + TopicNamesSameTenantAndNsRGs[ix];
            NonPersistentTopicNamesDifferentTenantAndNsRGs[ix] =
                    "non-persistent://" + TopicNamesDifferentTenantAndNsRGs[ix];
        }
    }
}
