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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@Slf4j
public class TransactionConsumeTest extends TransactionTestBase {

    private final static String CONSUME_TOPIC = "persistent://public/txn/txn-consume-test";
    private final static String NORMAL_MSG_CONTENT = "Normal - ";
    private final static String TXN_MSG_CONTENT = "Txn - ";

    @BeforeMethod
    public void setup() throws Exception {
        setBrokerCount(1);
        super.internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant("public",
                new TenantInfo(Sets.newHashSet(), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace("public/txn", 10);
        admin.topics().createNonPartitionedTopic(CONSUME_TOPIC);
    }

    @AfterMethod
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

        long mostSigBits = 2L;
        long leastSigBits = 5L;
        TxnID txnID = new TxnID(mostSigBits, leastSigBits);

        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(CONSUME_TOPIC, false).get().get();
        TransactionBuffer transactionBuffer = persistentTopic.getTransactionBuffer(true).get();
        log.info("transactionBuffer init finish.");

        sendNormalMessages(producer, 0, messageCntBeforeTxn);
        // append messages to TB
        appendTransactionMessages(txnID, transactionBuffer, transactionMessageCnt);
        sendNormalMessages(producer, messageCntBeforeTxn, messageCntAfterTxn);

        for (int i = 0; i < totalMsgCnt; i++) {
            if (i < (messageCntBeforeTxn + messageCntAfterTxn)) {
                // receive normal messages successfully
                Message<byte[]> message = exclusiveConsumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                log.info("Receive exclusive normal msg: {}" + new String(message.getData(), UTF_8));
                message = sharedConsumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                log.info("Receive shared normal msg: {}" + new String(message.getData(), UTF_8));
            } else {
                // can't receive transaction messages before commit
                Message<byte[]> message = exclusiveConsumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNull(message);
                message = sharedConsumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNull(message);
                log.info("Can't receive message before commit.");
            }
        }

        transactionBuffer.endTxnOnPartition(txnID, PulsarApi.TxnAction.COMMIT.getNumber());
        Thread.sleep(1000);
        log.info("Commit txn.");

        Map<String, Integer> exclusiveBatchIndexMap = new HashMap<>();
        Map<String, Integer> sharedBatchIndexMap = new HashMap<>();
        // receive transaction messages successfully after commit
        for (int i = 0; i < transactionMessageCnt; i++) {
            Message<byte[]> message = exclusiveConsumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertTrue(message.getMessageId() instanceof BatchMessageIdImpl);
            checkBatchIndex(exclusiveBatchIndexMap, (BatchMessageIdImpl) message.getMessageId());
            log.info("Receive txn exclusive id: {}, msg: {}", message.getMessageId(), new String(message.getData()));

            message = sharedConsumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertTrue(message.getMessageId() instanceof BatchMessageIdImpl);
            checkBatchIndex(sharedBatchIndexMap, (BatchMessageIdImpl) message.getMessageId());
            log.info("Receive txn shared id: {}, msg: {}", message.getMessageId(), new String(message.getData(), UTF_8));
        }
        log.info("TransactionConsumeTest noSortedTest finish.");
    }

    private void sendNormalMessages(Producer<byte[]> producer, int startMsgCnt, int messageCnt)
            throws PulsarClientException {
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage().value((NORMAL_MSG_CONTENT + (startMsgCnt + i)).getBytes(UTF_8)).send();
        }
    }

    private void appendTransactionMessages(TxnID txnID, TransactionBuffer tb, int transactionMsgCnt) {
        for (int i = 0; i < transactionMsgCnt; i++) {
            PulsarApi.MessageMetadata.Builder builder = PulsarApi.MessageMetadata.newBuilder();
            builder.setProducerName("producerName");
            builder.setSequenceId(i);
            builder.setTxnidMostBits(txnID.getMostSigBits());
            builder.setTxnidLeastBits(txnID.getLeastSigBits());
            builder.setPublishTime(System.currentTimeMillis());

            ByteBuf headerAndPayload = Commands.serializeMetadataAndPayload(
                    Commands.ChecksumType.Crc32c, builder.build(),
                    Unpooled.copiedBuffer((TXN_MSG_CONTENT + i).getBytes(UTF_8)));
            tb.appendBufferToTxn(txnID, i, 1, headerAndPayload);
        }
        log.info("append messages to TB finish.");
    }

    private void checkBatchIndex(Map<String, Integer> batchIndexMap, BatchMessageIdImpl messageId) {
        batchIndexMap.compute(messageId.getLedgerId() + ":" + messageId.getEntryId(),
            (key, value) -> {
                if (value == null) {
                    Assert.assertEquals(messageId.getBatchIndex(), 0);
                    return messageId.getBatchIndex();
                } else {
                    Assert.assertEquals(messageId.getBatchIndex(), value + 1);
                    return value + 1;
                }
            });
    }

}
