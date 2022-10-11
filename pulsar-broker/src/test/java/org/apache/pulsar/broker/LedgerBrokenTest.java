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
package org.apache.pulsar.broker;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.compaction.CompactedTopicImpl;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.reflect.WhiteboxImpl;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LedgerBrokenTest {

    // prefer inet4.
    private static final String LOCALHOST = Inet4Address.getLoopbackAddress().getHostAddress();
    private static final String CLUSTER = "broken_ledger_test";
    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = DEFAULT_TENANT + "/default";

    protected LocalBookkeeperEnsemble bkEnsemble;
    protected ServiceConfiguration pulsarConfig;
    protected PulsarService pulsarService;
    protected int brokerWebServicePort;
    protected int brokerServicePort;
    protected String metadataServiceUri;
    protected BookKeeper bookKeeperClient;
    protected String brokerUrl;
    protected String brokerServiceUrl;
    protected PulsarAdmin pulsarAdmin;
    protected PulsarClient pulsarClient;

    @BeforeClass
    protected void setup() throws Exception {
        log.info("--- Start cluster ---");
        startLocalBookie();
        initPulsarConfig();
        startPulsar();
    }

    @AfterClass
    protected void cleanup() throws Exception {
        log.info("--- Shutting down ---");
        silentStopPulsar();
        stopLocalBookie();
    }

    protected void startLocalBookie() throws Exception{
        log.info("===> Start bookie ");
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        metadataServiceUri = String.format("zk:%s:%s", LOCALHOST, bkEnsemble.getZookeeperPort());
        initBookieClient();
    }

    protected void initBookieClient() throws Exception {
        bookKeeperClient = new BookKeeper(String.format("%s:%s", LOCALHOST, bkEnsemble.getZookeeperPort()));
    }

    protected void stopLocalBookie() {
        log.info("===> Close bookie client");
        try {
            bookKeeperClient.close();
            // TODO delete bk files.
            // TODO delete zk files.
        } catch (Exception e){
            log.error("Close bookie client fail", e);
        }
        log.info("===> Stop bookie ");
        try {
            bkEnsemble.stop();
            // TODO delete bk files.
            // TODO delete zk files.
        } catch (Exception e){
            log.error("Stop bookie fail", e);
        }
    }

    protected void initPulsarConfig() throws Exception{
        pulsarConfig = new ServiceConfiguration();
        pulsarConfig.setAdvertisedAddress(LOCALHOST);
        pulsarConfig.setMetadataStoreUrl(metadataServiceUri);
        pulsarConfig.setClusterName(CLUSTER);
        pulsarConfig.setTransactionCoordinatorEnabled(false);
        pulsarConfig.setAllowAutoTopicCreation(true);
        pulsarConfig.setAllowAutoTopicCreationType("non-partitioned");
        pulsarConfig.setAutoSkipNonRecoverableData(true);
    }

    protected void startPulsar() throws Exception {
        log.info("===> Start pulsar ");
        pulsarService = new PulsarService(pulsarConfig);
        pulsarService.start();
        brokerWebServicePort = pulsarService.getListenPortHTTP().get();
        brokerServicePort = pulsarService.getBrokerListenPort().get();
        brokerUrl = String.format("http://%s:%s", LOCALHOST, brokerWebServicePort);
        brokerServiceUrl = String.format("pulsar://%s:%s", LOCALHOST, brokerServicePort);
        initPulsarAdmin();
        initPulsarClient();
        initDefaultNamespace();
    }

    protected void silentStopPulsar() throws Exception {
        log.info("===> Close pulsar client ");
        try {
            pulsarClient.close();
        }catch (Exception e){
            log.error("===> Close pulsar client fail", e);
        }
        log.info("===> Close pulsar admin ");
        try {
            pulsarAdmin.close();
        }catch (Exception e){
            log.error("===> Close pulsar admin fail", e);
        }
        log.info("===> Stop pulsar service ");
        try {
            pulsarService.close();
        }catch (Exception e){
            log.error("===> Stop pulsar service fail", e);
        }
    }

    protected void stopPulsar() throws Exception {
        log.info("===> Close pulsar client ");
        pulsarClient.close();
        log.info("===> Close pulsar admin ");
        pulsarAdmin.close();
        log.info("===> Stop pulsar service ");
        pulsarService.close();
    }

    protected void initPulsarAdmin() throws Exception {
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl).build();
    }

    protected void initPulsarClient() throws Exception {
        pulsarClient = PulsarClient.builder().serviceUrl(brokerServiceUrl).build();
    }

    protected void initDefaultNamespace() throws Exception {
        if (!pulsarAdmin.clusters().getClusters().contains(CLUSTER)) {
            pulsarAdmin.clusters().createCluster(CLUSTER, ClusterData.builder().serviceUrl(brokerServiceUrl).build());
        }
        if (!pulsarAdmin.tenants().getTenants().contains(DEFAULT_TENANT)){
            pulsarAdmin.tenants().createTenant(DEFAULT_TENANT,
                    TenantInfo.builder().allowedClusters(Collections.singleton(CLUSTER)).build());
        }
        if (!pulsarAdmin.namespaces().getNamespaces(DEFAULT_TENANT).contains(DEFAULT_NAMESPACE)) {
            pulsarAdmin.namespaces().createNamespace(DEFAULT_NAMESPACE, Collections.singleton(CLUSTER));
        }
    }

    @Test(timeOut = 30000)
    public void testTopicLedgerLost() throws Exception {
        String topicSimpleName = UUID.randomUUID().toString().replaceAll("-", "");
        String subName = UUID.randomUUID().toString().replaceAll("-", "");
        String topicName = String.format("persistent://%s/%s", DEFAULT_NAMESPACE, topicSimpleName);

        log.info("===> ensure producer & consumer works first time");
        ensureProducerAndConsumerWork(topicName, subName, false);

        log.info("===> send many messages");
        sendManyMessages(topicName, 10);
        pulsarAdmin.topics().unload(topicName);
        sendManyMessages(topicName, 10);

        log.info("===> stop pulsar service and delete ledgers");
        PersistentTopicInternalStats topicInternalStats = pulsarAdmin.topics().getInternalStats(topicName);
        silentStopPulsar();
        for (ManagedLedgerInternalStats.LedgerInfo ledgerInfo : topicInternalStats.ledgers){
            bookKeeperClient.deleteLedger(ledgerInfo.ledgerId);
        }

        log.info("===> restart broker.");
        startPulsar();

        log.info("===> verify producer and consumer works.");
        ensureProducerAndConsumerWork(topicName, subName, false);

        // cleanup
        pulsarAdmin.topics().delete(topicName);
    }

    @Test(timeOut = 30000)
    public void testCursorLedgerLost() throws Exception {
        String topicSimpleName = UUID.randomUUID().toString().replaceAll("-", "");
        String subName = UUID.randomUUID().toString().replaceAll("-", "");
        String topicName = String.format("persistent://%s/%s", DEFAULT_NAMESPACE, topicSimpleName);

        log.info("===> ensure producer & consumer works first time");
        ensureProducerAndConsumerWork(topicName, subName, false);

        log.info("===> send many messages");
        List<MessageIdImpl> messageIds = new ArrayList<>();
        messageIds.addAll(sendManyMessages(topicName, 5));
        pulsarAdmin.topics().unload(topicName);
        messageIds.addAll(sendManyMessages(topicName, 5));

        log.info("===> receive messages and skip ack");
        receiveAndAckSomeMessage(topicSimpleName, subName, 10, 1,5,10);

        log.info("===> stop pulsar service and delete cursor ledgers");
        PersistentTopicInternalStats topicInternalStats = pulsarAdmin.topics().getInternalStats(topicName);
        stopPulsar();
        ManagedLedgerInternalStats.CursorStats cursorStats = topicInternalStats.cursors.get(subName);
        bookKeeperClient.deleteLedger(cursorStats.cursorLedger);

        log.info("===> restart broker.");
        startPulsar();

        log.info("===> verify producer and consumer works.");
        ensureProducerAndConsumerWork(topicName, subName, false);

        // cleanup
        pulsarAdmin.topics().delete(topicName);
    }

    @Test(timeOut = 30000)
    public void testCompactionLedgerLost() throws Exception {
        String topicSimpleName = UUID.randomUUID().toString().replaceAll("-", "");
        String subName = UUID.randomUUID().toString().replaceAll("-", "");
        String topicName = String.format("persistent://%s/%s", DEFAULT_NAMESPACE, topicSimpleName);

        log.info("===> ensure producer & consumer works first time");
        ensureProducerAndConsumerWork(topicName, subName, true);

        log.info("===> send many messages");
        List<MessageIdImpl> messageIds = new ArrayList<>();
        messageIds.addAll(sendManyMessages(topicName, 5));
        pulsarAdmin.topics().unload(topicName);
        messageIds.addAll(sendManyMessages(topicName, 5));
        pulsarAdmin.topics().unload(topicName);
        messageIds.addAll(sendManyMessages(topicName, 5));

        log.info("===> stop pulsar service and delete compaction ledger");
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsarService.getBrokerService().getTopics().get(topicName).join().get();
        triggerAndWaitCompaction(persistentTopic);
        final CompactedTopicImpl compactedTopic = (CompactedTopicImpl) persistentTopic.getCompactedTopic();
        final long compactionLedgerId = compactedTopic.getCompactedTopicContext().get().getLedger().getId();
        stopPulsar();
        bookKeeperClient.deleteLedger(compactionLedgerId);

        log.info("===> restart broker.");
        startPulsar();

        log.info("===> verify producer and consumer works.");
        ensureProducerAndConsumerWork(topicName, subName, true);

        // cleanup
        pulsarAdmin.topics().delete(topicName);
    }

    private void triggerAndWaitCompaction(PersistentTopic persistentTopic){
        final Position lastConfirmedEntry = persistentTopic.getManagedLedger().getLastConfirmedEntry();
        Awaitility.await().until(() -> {
            boolean compactionFinished = false;
            try {
                // ensure all message has been ack.
                PersistentSubscription persistentSubscription =
                        persistentTopic.getSubscription(Compactor.COMPACTION_SUBSCRIPTION);
                if (persistentSubscription == null){
                    return false;
                }
                ManagedCursorImpl cursor = (ManagedCursorImpl) persistentSubscription.getCursor();
                Position markDeletedPos = cursor.getPersistentMarkDeletedPosition();
                if (markDeletedPos.getLedgerId() != lastConfirmedEntry.getLedgerId()) {
                    return false;
                }
                if (markDeletedPos.getEntryId() != lastConfirmedEntry.getEntryId()) {
                    return false;
                }
                return (compactionFinished = true);
            }finally {
                if (!compactionFinished){
                    // trigger compaction.
                    try {
                        persistentTopic.triggerCompaction();
                    } catch (PulsarServerException | BrokerServiceException.AlreadyRunningException e) {
                    }
                    CompletableFuture<Long> currentCompaction =
                            WhiteboxImpl.getInternalState(persistentTopic, "currentCompaction");
                    currentCompaction.join();
                }
            }
        });
    }

    private void receiveAndAckSomeMessage(String topicName, String subName, int count, Integer...ackIndexes)
            throws Exception {
        final Consumer consumer = pulsarClient.newConsumer(Schema.JSON(String.class))
                .subscriptionType(SubscriptionType.Failover)
                .isAckReceiptEnabled(true)
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();
        final Set<Integer> ackIndexSet = new TreeSet(Arrays.asList(ackIndexes));
        final AtomicInteger counter = new AtomicInteger(count);
        final AtomicInteger index = new AtomicInteger();
        Awaitility.await().until(() -> {
            Message<String> msg = consumer.receive();
            if (counter.get() == 0){
                return true;
            }
            if (msg == null){
                return false;
            }
            counter.decrementAndGet();
            if (ackIndexSet.contains(index.get())){
                consumer.acknowledge(msg);
            }
            index.incrementAndGet();
            return false;
        });
        consumer.close();
    }

    private void ensureProducerAndConsumerWork(String topicName, String subName, boolean readCompacted)
            throws Exception {
        final Consumer consumer = pulsarClient.newConsumer(Schema.JSON(String.class))
                .subscriptionType(SubscriptionType.Failover)
                .isAckReceiptEnabled(true)
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .readCompacted(readCompacted)
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();
        final Producer<String> producer = pulsarClient.newProducer(Schema.JSON(String.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        final String msgContent = UUID.randomUUID().toString().replaceAll("-", "");
        final MessageIdImpl messageIdSent = (MessageIdImpl) producer.newMessage()
                .key(msgContent).value(String.format("Msg-%s", msgContent))
                .send();
        try {
            Awaitility.await().until(() -> {
                Message msg = consumer.receive();
                MessageIdImpl messageIdReceived = (MessageIdImpl) msg.getMessageId();
                if (messageIdReceived.getLedgerId() != messageIdSent.getLedgerId()) {
                    return false;
                }
                if (messageIdReceived.getEntryId() != messageIdSent.getEntryId()) {
                    return false;
                }
                return true;
            });
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private List<MessageIdImpl> sendManyMessages(String topic, int msgCount) throws Exception {
        List<MessageIdImpl> messageIdList = new ArrayList<>();
        final Producer<String> producer = pulsarClient.newProducer(Schema.JSON(String.class))
                .topic(topic)
                .enableBatching(false)
                .create();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < msgCount; i++){
            String messageSuffix = String.format("%s-%s", timestamp, i);
            MessageIdImpl messageIdSent = (MessageIdImpl) producer.newMessage()
                    .key(String.format("Key-%s", messageSuffix))
                    .value(String.format("Msg-%s", messageSuffix))
                    .send();
            messageIdList.add(messageIdSent);
        }
        producer.close();
        return messageIdList;
    }
}
