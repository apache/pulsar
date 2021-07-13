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
package org.apache.pulsar.broker.transaction;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionConsumeTest extends TransactionTestBase {

    private static final String CONSUME_TOPIC = "persistent://public/txn/txn-consume-test";
    private static final String NORMAL_MSG_CONTENT = "Normal - ";
    private static final String TXN_MSG_CONTENT = "Txn - ";

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        setBrokerCount(1);
        super.internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace("public/txn", 10);
        admin.topics().createNonPartitionedTopic(CONSUME_TOPIC);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void noSortedTest() throws Exception {
        int messageCntBeforeTxn = 10;
        int transactionMessageCnt = 10;
        int messageCntAfterTxn = 10;
        int totalMsgCnt = messageCntBeforeTxn + transactionMessageCnt + messageCntAfterTxn;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(CONSUME_TOPIC)
                .create();

        @Cleanup
        Consumer<byte[]> exclusiveConsumer = pulsarClient.newConsumer()
                .topic(CONSUME_TOPIC)
                .subscriptionName("exclusive-test")
                .subscribe();

        @Cleanup
        Consumer<byte[]> sharedConsumer = pulsarClient.newConsumer()
                .topic(CONSUME_TOPIC)
                .subscriptionName("shared-test")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Awaitility.await().until(exclusiveConsumer::isConnected);
        Awaitility.await().until(sharedConsumer::isConnected);

        long mostSigBits = 2L;
        long leastSigBits = 5L;
        TxnID txnID = new TxnID(mostSigBits, leastSigBits);

        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(CONSUME_TOPIC, false).get().get();
        log.info("transactionBuffer init finish.");

        List<String> sendMessageList = new ArrayList<>();
        sendNormalMessages(producer, 0, messageCntBeforeTxn, sendMessageList);
        appendTransactionMessages(txnID, persistentTopic, transactionMessageCnt, sendMessageList);
        sendNormalMessages(producer, messageCntBeforeTxn, messageCntAfterTxn, sendMessageList);

        Message<byte[]> message;
        for (int i = 0; i < totalMsgCnt; i++) {
            if (i < messageCntBeforeTxn) {
                // receive normal messages successfully
                message = exclusiveConsumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                log.info("Receive exclusive normal msg: {}" + new String(message.getData(), UTF_8));
                message = sharedConsumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                log.info("Receive shared normal msg: {}" + new String(message.getData(), UTF_8));
            } else {
                // can't receive transaction messages before commit
                message = exclusiveConsumer.receive(500, TimeUnit.MILLISECONDS);
                Assert.assertNull(message);
                log.info("exclusive consumer can't receive message before commit.");

                message = sharedConsumer.receive(500, TimeUnit.MILLISECONDS);
                Assert.assertNull(message);
                log.info("shared consumer can't receive message before commit.");
            }
        }

        persistentTopic.endTxn(txnID, TxnAction.COMMIT_VALUE, 0L).get();
        log.info("Commit txn.");

        // receive transaction messages successfully after commit
        for (int i = 0; i < transactionMessageCnt + messageCntAfterTxn; i++) {
            message = exclusiveConsumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            log.info("Receive txn exclusive id: {}, msg: {}", message.getMessageId(), new String(message.getData()));

            message = sharedConsumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            log.info("Receive txn shared id: {}, msg: {}", message.getMessageId(), new String(message.getData()));
        }
        log.info("TransactionConsumeTest noSortedTest finish.");
    }

    @Test
    public void sortedTest() throws Exception {
        int messageCntBeforeTxn = 10;
        int transactionMessageCnt = 10;
        int messageCntAfterTxn = 10;
        int totalMsgCnt = messageCntBeforeTxn + transactionMessageCnt + messageCntAfterTxn;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(CONSUME_TOPIC)
                .create();

        @Cleanup
        Consumer<byte[]> exclusiveConsumer = pulsarClient.newConsumer()
                .topic(CONSUME_TOPIC)
                .subscriptionName("exclusive-test")
                .subscribe();

        @Cleanup
        Consumer<byte[]> sharedConsumer = pulsarClient.newConsumer()
                .topic(CONSUME_TOPIC)
                .subscriptionName("shared-test")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        Awaitility.await().until(exclusiveConsumer::isConnected);
        Awaitility.await().until(sharedConsumer::isConnected);

        long mostSigBits = 2L;
        long leastSigBits = 5L;
        TxnID txnID = new TxnID(mostSigBits, leastSigBits);

        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(CONSUME_TOPIC, false).get().get();

        List<String> sendMessageList = new ArrayList<>();
        sendNormalMessages(producer, 0, messageCntBeforeTxn, sendMessageList);
        // append messages to TB
        appendTransactionMessages(txnID, persistentTopic, transactionMessageCnt, sendMessageList);
        persistentTopic.endTxn(txnID, TxnAction.COMMIT_VALUE, 0L).get();
        log.info("Commit txn.");
        sendNormalMessages(producer, messageCntBeforeTxn, messageCntAfterTxn, sendMessageList);

        Message<byte[]> message;
        for (int i = 0; i < totalMsgCnt; i++) {
            message = exclusiveConsumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertEquals(sendMessageList.get(i), new String(message.getData()));
            log.info("Receive exclusive normal msg: {}, index: {}", new String(message.getData(), UTF_8), i);
            message = sharedConsumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertEquals(sendMessageList.get(i), new String(message.getData()));
            log.info("Receive shared normal msg: {}, index: {}", new String(message.getData(), UTF_8), i);
        }
        log.info("TransactionConsumeTest sortedTest finish.");
    }

    private void sendNormalMessages(Producer<byte[]> producer, int startMsgCnt,
                                    int messageCnt, List<String> sendMessageList)
            throws PulsarClientException {
        for (int i = 0; i < messageCnt; i++) {
            String msg = NORMAL_MSG_CONTENT + (startMsgCnt + i);
            sendMessageList.add(msg);
            producer.newMessage().value(msg.getBytes(UTF_8)).send();
        }
    }

    private List<MessageIdData> appendTransactionMessages(
            TxnID txnID, PersistentTopic topic, int transactionMsgCnt, List<String> sendMessageList)
            throws ExecutionException, InterruptedException {
        List<MessageIdData> positionList = new ArrayList<>();
        for (int i = 0; i < transactionMsgCnt; i++) {
            final int j = i;
            MessageMetadata metadata = new MessageMetadata()
                    .setProducerName("producerName")
                    .setSequenceId(i)
                    .setTxnidMostBits(txnID.getMostSigBits())
                    .setTxnidLeastBits(txnID.getLeastSigBits())
                    .setPublishTime(System.currentTimeMillis());

            String msg = TXN_MSG_CONTENT + i;
            sendMessageList.add(msg);
            ByteBuf headerAndPayload = Commands.serializeMetadataAndPayload(
                    Commands.ChecksumType.Crc32c, metadata,
                    Unpooled.copiedBuffer(msg.getBytes(UTF_8)));
            CompletableFuture<PositionImpl> completableFuture = new CompletableFuture<>();
            topic.publishTxnMessage(txnID, headerAndPayload, new Topic.PublishContext() {

                @Override
                public String getProducerName() {
                    return "test";
                }

                public long getSequenceId() {
                    return j + 30;
                }

                /**
                 * Return the producer name for the original producer.
                 *
                 * For messages published locally, this will return the same local producer name, though in case of replicated
                 * messages, the original producer name will differ
                 */
                public String getOriginalProducerName() {
                    return "test";
                }

                public long getOriginalSequenceId() {
                    return j + 30;
                }

                public long getHighestSequenceId() {
                    return  j + 30;
                }

                public long getOriginalHighestSequenceId() {
                    return  j + 30;
                }

                public long getNumberOfMessages() {
                    return  j + 30;
                }

                @Override
                public void completed(Exception e, long ledgerId, long entryId) {
                    completableFuture.complete(PositionImpl.get(ledgerId, entryId));
                }
            });
            positionList.add(new MessageIdData().setLedgerId(completableFuture.get()
                    .getLedgerId()).setEntryId(completableFuture.get().getEntryId()));
        }
        log.info("append messages to TB finish.");
        return positionList;
    }

}
