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

import static org.testng.Assert.*;
import java.io.Serializable;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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
        pulsarConfig.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
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
            pulsarAdmin.clusters().createCluster(CLUSTER, ClusterData.builder().serviceUrl(brokerUrl).build());
        }
        if (!pulsarAdmin.tenants().getTenants().contains(DEFAULT_TENANT)){
            pulsarAdmin.tenants().createTenant(DEFAULT_TENANT,
                    TenantInfo.builder().allowedClusters(Collections.singleton(CLUSTER)).build());
        }
        if (!pulsarAdmin.namespaces().getNamespaces(DEFAULT_TENANT).contains(DEFAULT_NAMESPACE)) {
            pulsarAdmin.namespaces().createNamespace(DEFAULT_NAMESPACE, Collections.singleton(CLUSTER));
        }
    }

    @DataProvider(name = "lostSchemaLedgerIndexes")
    public Object[][] lostSchemaLedgerIndexes(){
        return new Object[][]{
                {Arrays.asList(0,1)},
                {Arrays.asList(0)},
                {Arrays.asList(1)}
        };
    }

    @Test(dataProvider = "lostSchemaLedgerIndexes", timeOut = 30000)
    public void testSchemaLedgerLost(List<Integer> lostSchemaLedgerIndexes) throws Exception {
        String topicSimpleName = UUID.randomUUID().toString().replaceAll("-", "");
        String subName = UUID.randomUUID().toString().replaceAll("-", "");
        String topicName = String.format("persistent://%s/%s", DEFAULT_NAMESPACE, topicSimpleName);
        SchemaCompatibilityStrategy originalStrategy =
                pulsarAdmin.namespaces().getPolicies(DEFAULT_NAMESPACE).schema_compatibility_strategy;
        pulsarAdmin.namespaces().setSchemaCompatibilityStrategy(DEFAULT_NAMESPACE,
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        log.info("===> ensure consumer and producer works");
        ensureProducerAndConsumerWorkWithStringSchema(topicName, subName, false);

        log.info("===> registry two schemas");
        List<MessageIdImpl> messageIds = new ArrayList<>();
        messageIds.addAll(sendManyMessagesWithStringSchema(topicName, 5));
        printAllSchemas(topicName);
        pulsarAdmin.topics().unload(topicName);
        messageIds.addAll(sendManyMessagesWithStringWrapSchema(topicName, 5));
        printAllSchemas(topicName);
        pulsarAdmin.topics().unload(topicName);

        log.info("===> stop pulsar service and delete schema ledgers");
        PersistentTopicInternalStats topicInternalStats = pulsarAdmin.topics().getInternalStats(topicName);
        assertEquals(topicInternalStats.schemaLedgers.size(), 2);
        stopPulsar();
        for (int i = 0; i < topicInternalStats.schemaLedgers.size(); i++){
            if (lostSchemaLedgerIndexes.contains(i)){
                bookKeeperClient.deleteLedger(topicInternalStats.schemaLedgers.get(i).ledgerId);
            }
        }

        log.info("===> restart broker.");
        startPulsar();

        log.info("===> verify producer and consumer works.");
        int skippedMessageCount = ensureProducerAndConsumerWorkWithStringWrapSchema(topicName, subName, false);
        assertTrue(skippedMessageCount >= 10);
        printAllSchemas(topicName);

        // cleanup
        pulsarAdmin.namespaces().setSchemaCompatibilityStrategy(DEFAULT_NAMESPACE, originalStrategy);
        pulsarAdmin.topics().delete(topicName);
    }

    private void printAllSchemas(String topicName){
        String base = TopicName.get(topicName).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        List<CompletableFuture<SchemaRegistry.SchemaAndMetadata>> list =
                pulsarService.getSchemaRegistryService().getAllSchemas(id).join();
        StringBuilder stringBuilder = new StringBuilder("===> Print all schemas, schemaId: ")
                .append(id).append(", count: ").append(list.size()).append("[");
        for (int i = 0; i < list.size(); i++){
            SchemaRegistry.SchemaAndMetadata schemaAndMetadata = list.get(i).join();
            stringBuilder.append("version: ").append(schemaAndMetadata.version).append(", ");
            stringBuilder.append("schema-type: ").append(schemaAndMetadata.schema.getType());
            if (i != list.size() - 1){
                stringBuilder.append(", ");
            }
        }
        stringBuilder.append("]");
        log.info(stringBuilder.toString());
    }

    @AllArgsConstructor
    @Data
    private static class StringWrap implements Serializable {
        private String name;
    }

    /**
     * @return How many messages skipped the historical backlog.
     */
    private int ensureProducerAndConsumerWorkWithStringSchema(String topicName, String subName, boolean readCompacted)
            throws Exception {
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.JSON(String.class))
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
            AtomicInteger skippedMessageCount = new AtomicInteger();
            Awaitility.await().until(() -> {
                Message msg = consumer.receive();
                MessageIdImpl messageIdReceived = (MessageIdImpl) msg.getMessageId();
                if (messageIdReceived.getLedgerId() != messageIdSent.getLedgerId()) {
                    skippedMessageCount.incrementAndGet();
                    return false;
                }
                if (messageIdReceived.getEntryId() != messageIdSent.getEntryId()) {
                    skippedMessageCount.incrementAndGet();
                    return false;
                }
                return true;
            });
            return skippedMessageCount.get();
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private int ensureProducerAndConsumerWorkWithStringWrapSchema(String topicName, String subName,
                                                                  boolean readCompacted) throws Exception {
        final Consumer<StringWrap> consumer = pulsarClient.newConsumer(Schema.JSON(StringWrap.class))
                .subscriptionType(SubscriptionType.Failover)
                .isAckReceiptEnabled(true)
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .readCompacted(readCompacted)
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();
        final Producer<StringWrap> producer = pulsarClient.newProducer(Schema.JSON(StringWrap.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        final String msgContent = UUID.randomUUID().toString().replaceAll("-", "");
        final MessageIdImpl messageIdSent = (MessageIdImpl) producer.newMessage()
                .key(msgContent).value(new StringWrap(String.format("Msg-%s", msgContent)))
                .send();
        try {
            AtomicInteger skippedMessageCount = new AtomicInteger();
            Awaitility.await().until(() -> {
                Message<StringWrap> msg = consumer.receive();
                MessageIdImpl messageIdReceived = (MessageIdImpl) msg.getMessageId();
                if (messageIdReceived.getLedgerId() != messageIdSent.getLedgerId()) {
                    skippedMessageCount.incrementAndGet();
                    return false;
                }
                if (messageIdReceived.getEntryId() != messageIdSent.getEntryId()) {
                    skippedMessageCount.incrementAndGet();
                    return false;
                }
                return true;
            });
            return skippedMessageCount.get();
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private List<MessageIdImpl> sendManyMessagesWithStringSchema(String topic, int msgCount) throws Exception {
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

    private List<MessageIdImpl> sendManyMessagesWithStringWrapSchema(String topic, int msgCount) throws Exception {
        List<MessageIdImpl> messageIdList = new ArrayList<>();
        final Producer<StringWrap> producer = pulsarClient.newProducer(Schema.JSON(StringWrap.class))
                .topic(topic)
                .enableBatching(false)
                .create();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < msgCount; i++){
            String messageSuffix = String.format("%s-%s", timestamp, i);
            MessageIdImpl messageIdSent = (MessageIdImpl) producer.newMessage()
                    .key(String.format("Key-%s", messageSuffix))
                    .value(new StringWrap(String.format("Msg-%s", messageSuffix)))
                    .send();
            messageIdList.add(messageIdSent);
        }
        producer.close();
        return messageIdList;
    }
}
