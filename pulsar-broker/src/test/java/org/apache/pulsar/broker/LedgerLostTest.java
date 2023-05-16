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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LedgerLostTest {

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
        log.info("Start bookie ");
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        metadataServiceUri = String.format("zk:%s:%s", LOCALHOST, bkEnsemble.getZookeeperPort());
        initBookieClient();
    }

    protected void initBookieClient() throws Exception {
        bookKeeperClient = new BookKeeper(String.format("%s:%s", LOCALHOST, bkEnsemble.getZookeeperPort()));
    }

    protected void stopLocalBookie() {
        log.info("Close bookie client");
        try {
            bookKeeperClient.close();
        } catch (Exception e){
            log.error("Close bookie client fail", e);
        }
        log.info("Stop bookie ");
        try {
            bkEnsemble.stop();
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
        pulsarConfig.setSystemTopicEnabled(true);
        pulsarConfig.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        pulsarConfig.setAutoSkipNonRecoverableData(true);
        pulsarConfig.setManagedLedgerDefaultMarkDeleteRateLimit(Integer.MAX_VALUE);
        pulsarConfig.setBrokerDeleteInactiveTopicsEnabled(false);
        pulsarConfig.setAcknowledgmentAtBatchIndexLevelEnabled(true);
        pulsarConfig.setLazyCursorRecovery(false);
    }

    protected void startPulsar() throws Exception {
        log.info("Start pulsar ");
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
        log.info("Close pulsar client ");
        try {
            pulsarClient.close();
        }catch (Exception e){
            log.error("Close pulsar client fail", e);
        }
        log.info("Close pulsar admin ");
        try {
            pulsarAdmin.close();
        }catch (Exception e){
            log.error("Close pulsar admin fail", e);
        }
        log.info("Stop pulsar service ");
        try {
            pulsarService.close();
        }catch (Exception e){
            log.error("Stop pulsar service fail", e);
        }
    }

    protected void stopPulsar() throws Exception {
        log.info("Close pulsar client ");
        pulsarClient.close();
        log.info("Close pulsar admin ");
        pulsarAdmin.close();
        log.info("Stop pulsar service ");
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

    @DataProvider(name = "batchEnabled")
    public Object[][] batchEnabled(){
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(timeOut = 30000, dataProvider = "batchEnabled")
    public void testTopicLedgerLost(boolean enabledBatch) throws Exception {
        String topicSimpleName = UUID.randomUUID().toString().replaceAll("-", "");
        String subName = UUID.randomUUID().toString().replaceAll("-", "");
        String topicName = String.format("persistent://%s/%s", "public/default", topicSimpleName);

        log.info("create topic and subscription.");
        Consumer sub = createConsumer(topicName, subName, enabledBatch);
        sub.redeliverUnacknowledgedMessages();
        sub.close();

        log.info("send many messages.");
        int ledgerCount = 3;
        int messageCountPerLedger = enabledBatch ? 25 : 5;
        int messageCountPerEntry = enabledBatch ? 5 : 1;
        List<MessageIdImpl>[] sendMessages =
                sendManyMessages(topicName, ledgerCount, messageCountPerLedger, messageCountPerEntry);
        int sendMessageCount = Arrays.asList(sendMessages).stream()
                .flatMap(s -> s.stream()).collect(Collectors.toList()).size();
        log.info("send {} messages", sendMessageCount);

        log.info("make individual ack.");
        ConsumerAndReceivedMessages consumerAndReceivedMessages1 =
                waitConsumeAndAllMessages(topicName, subName, enabledBatch,false);
        List<MessageIdImpl>[] messageIds = consumerAndReceivedMessages1.messageIds;
        Consumer consumer = consumerAndReceivedMessages1.consumer;
        MessageIdImpl individualPosition = messageIds[1].get(messageCountPerEntry - 1);
        MessageIdImpl expectedMarkDeletedPosition =
                new MessageIdImpl(messageIds[0].get(0).getLedgerId(), messageIds[0].get(0).getEntryId(), -1);
        MessageIdImpl lastPosition =
                new MessageIdImpl(messageIds[2].get(4).getLedgerId(), messageIds[2].get(4).getEntryId(), -1);
        consumer.acknowledge(individualPosition);
        consumer.acknowledge(expectedMarkDeletedPosition);
        waitPersistentCursorLedger(topicName, subName, expectedMarkDeletedPosition.getLedgerId(),
                expectedMarkDeletedPosition.getEntryId());
        consumer.close();

        log.info("Make lost ledger [{}].", individualPosition.getLedgerId());
        pulsarService.getBrokerService().getTopic(topicName, false).get().get().close(false);
        bookKeeperClient.deleteLedger(individualPosition.getLedgerId());

        log.info("send some messages.");
        sendManyMessages(topicName, 3, messageCountPerEntry);

        log.info("receive all messages then verify mark deleted position");
        ConsumerAndReceivedMessages consumerAndReceivedMessages2 =
                waitConsumeAndAllMessages(topicName, subName, enabledBatch, true);
        waitMarkDeleteLargeAndEquals(topicName, subName, lastPosition.getLedgerId(), lastPosition.getEntryId());

        // cleanup
        consumerAndReceivedMessages2.consumer.close();
        pulsarAdmin.topics().delete(topicName);
    }

    private ManagedCursorImpl getCursor(String topicName, String subName) throws Exception {
        PersistentSubscription subscription_ =
                (PersistentSubscription) pulsarService.getBrokerService().getTopic(topicName, false)
                        .get().get().getSubscription(subName);
        return  (ManagedCursorImpl) subscription_.getCursor();
    }

    private void waitMarkDeleteLargeAndEquals(String topicName, String subName, final long markDeletedLedgerId,
                                            final long markDeletedEntryId) throws Exception {
        Awaitility.await().untilAsserted(() -> {
            Position persistentMarkDeletedPosition = getCursor(topicName, subName).getMarkDeletedPosition();
            Assert.assertTrue(persistentMarkDeletedPosition.getLedgerId() >= markDeletedLedgerId);
            if (persistentMarkDeletedPosition.getLedgerId() > markDeletedLedgerId){
                return;
            }
            Assert.assertTrue(persistentMarkDeletedPosition.getEntryId() >= markDeletedEntryId);
        });
    }

    private void waitPersistentCursorLedger(String topicName, String subName, final long markDeletedLedgerId,
                                            final long markDeletedEntryId) throws Exception {
        Awaitility.await().untilAsserted(() -> {
            Position persistentMarkDeletedPosition = getCursor(topicName, subName).getPersistentMarkDeletedPosition();
            Assert.assertEquals(persistentMarkDeletedPosition.getLedgerId(), markDeletedLedgerId);
            Assert.assertEquals(persistentMarkDeletedPosition.getEntryId(), markDeletedEntryId);
        });
    }

    private List<MessageIdImpl>[] sendManyMessages(String topicName, int ledgerCount, int messageCountPerLedger,
                                                   int messageCountPerEntry) throws Exception {
        List<MessageIdImpl>[] messageIds = new List[ledgerCount];
        for (int i = 0; i < ledgerCount; i++){
            pulsarAdmin.topics().unload(topicName);
            if (messageCountPerEntry == 1) {
                messageIds[i] = sendManyMessages(topicName, messageCountPerLedger);
            } else {
                messageIds[i] = sendManyBatchedMessages(topicName, messageCountPerEntry,
                        messageCountPerLedger / messageCountPerEntry);
            }
        }
        return messageIds;
    }

    private List<MessageIdImpl> sendManyMessages(String topicName, int messageCountPerLedger,
                                                   int messageCountPerEntry) throws Exception {
        if (messageCountPerEntry == 1) {
            return sendManyMessages(topicName, messageCountPerLedger);
        } else {
            return sendManyBatchedMessages(topicName, messageCountPerEntry,
                    messageCountPerLedger / messageCountPerEntry);
        }
    }

    private List<MessageIdImpl> sendManyMessages(String topicName, int msgCount) throws Exception {
        List<MessageIdImpl> messageIdList = new ArrayList<>();
        final Producer<String> producer = pulsarClient.newProducer(Schema.JSON(String.class))
                .topic(topicName)
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

    private List<MessageIdImpl> sendManyBatchedMessages(String topicName, int msgCountPerEntry, int entryCount)
            throws Exception {
        Producer<String> producer = pulsarClient.newProducer(Schema.JSON(String.class))
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(Integer.MAX_VALUE, TimeUnit.SECONDS)
                .batchingMaxMessages(Integer.MAX_VALUE)
                .create();
        List<CompletableFuture<MessageId>> messageIdFutures = new ArrayList<>();
        for (int i = 0; i < entryCount; i++){
            for (int j = 0; j < msgCountPerEntry; j++){
                CompletableFuture<MessageId> messageIdFuture =
                        producer.newMessage().value(String.format("entry-seq[%s], batch_index[%s]", i, j)).sendAsync();
                messageIdFutures.add(messageIdFuture);
            }
            producer.flush();
        }
        producer.close();
        FutureUtil.waitForAll(messageIdFutures).get();
        return messageIdFutures.stream().map(f -> (MessageIdImpl)f.join()).collect(Collectors.toList());
    }

    private ConsumerAndReceivedMessages waitConsumeAndAllMessages(String topicName, String subName,
                                                            final boolean enabledBatch,
                                                            boolean ack) throws Exception {
        List<MessageIdImpl> messageIds = new ArrayList<>();
        final Consumer consumer = createConsumer(topicName, subName, enabledBatch);
        while (true){
            Message message = consumer.receive(5, TimeUnit.SECONDS);
            if (message != null){
                messageIds.add((MessageIdImpl) message.getMessageId());
                if (ack) {
                    consumer.acknowledge(message);
                }
            } else {
                break;
            }
        }
        log.info("receive {} messages", messageIds.size());
        return new ConsumerAndReceivedMessages(consumer, sortMessageId(messageIds, enabledBatch));
    }

    @AllArgsConstructor
    private static class ConsumerAndReceivedMessages {
        private Consumer consumer;
        private List<MessageIdImpl>[] messageIds;
    }

    private List<MessageIdImpl>[] sortMessageId(List<MessageIdImpl> messageIds, boolean enabledBatch){
        Map<Long, List<MessageIdImpl>> map = messageIds.stream().collect(Collectors.groupingBy(v -> v.getLedgerId()));
        TreeMap<Long, List<MessageIdImpl>> sortedMap = new TreeMap<>(map);
        List<MessageIdImpl>[] res = new List[sortedMap.size()];
        Iterator<Map.Entry<Long, List<MessageIdImpl>>> iterator = sortedMap.entrySet().iterator();
        for (int i = 0; i < sortedMap.size(); i++){
            res[i] = iterator.next().getValue();
        }
        for (List<MessageIdImpl> list : res){
            list.sort((m1, m2) -> {
                if (enabledBatch){
                    BatchMessageIdImpl mb1 = (BatchMessageIdImpl) m1;
                    BatchMessageIdImpl mb2 = (BatchMessageIdImpl) m2;
                    return (int) (mb1.getLedgerId() * 1000000 + mb1.getEntryId() * 1000 + mb1.getBatchIndex() -
                            mb2.getLedgerId() * 1000000 + mb2.getEntryId() * 1000 + mb2.getBatchIndex());
                }
                return (int) (m1.getLedgerId() * 1000 + m1.getEntryId() -
                        m2.getLedgerId() * 1000 + m2.getEntryId());
            });
        }
        return res;
    }

    private Consumer<String> createConsumer(String topicName, String subName, boolean enabledBatch) throws Exception {
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.JSON(String.class))
                .autoScaledReceiverQueueSizeEnabled(false)
                .subscriptionType(SubscriptionType.Failover)
                .isAckReceiptEnabled(true)
                .enableBatchIndexAcknowledgment(enabledBatch)
                .receiverQueueSize(1000)
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();
        return consumer;
    }
}
