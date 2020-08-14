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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.coordinator.TransactionMetaStoreTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@Slf4j
public class TransactionConsumeTest extends TransactionMetaStoreTestBase {

    private final static String CONSUME_TOPIC = "persistent://public/txn/txn-consume-test";
    private final static String NORMAL_MSG_CONTENT = "Normal - ";
    private final static String TXN_MSG_CONTENT = "Txn - ";

    @BeforeClass
    public void init() throws Exception {
        BROKER_COUNT = 1;
        super.setup();

        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        pulsarAdmins[0].tenants().createTenant("public", new TenantInfo(Sets.newHashSet(), Sets.newHashSet("my-cluster")));
        pulsarAdmins[0].namespaces().createNamespace("public/txn", 10);
        pulsarAdmins[0].topics().createNonPartitionedTopic(CONSUME_TOPIC);
    }

    @Test
    public void noSortedTest() throws Exception {
        int messageCntBeforeTxn = 10;
        int transactionMessageCnt = 10;
        int messageCntAfterTxn = 10;
        int totalMsgCnt = messageCntBeforeTxn + transactionMessageCnt + messageCntAfterTxn;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(CONSUME_TOPIC)
                .create();

        Consumer<byte[]> exclusiveConsumer = pulsarClient.newConsumer()
                .topic(CONSUME_TOPIC)
                .subscriptionName("exclusive-test")
                .subscribe();

        Consumer<byte[]> sharedConsumer = pulsarClient.newConsumer()
                .topic(CONSUME_TOPIC)
                .subscriptionName("shared-test")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        long mostSigBits = 2L;
        long leastSigBits = 5L;
        TxnID txnID = new TxnID(mostSigBits, leastSigBits);

        PersistentTopic persistentTopic = (PersistentTopic) pulsarServices[0].getBrokerService()
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

        for (int i = 0; i < transactionMessageCnt; i++) {
            Message<byte[]> message = exclusiveConsumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            log.info("Receive txn exclusive msg: {}", new String(message.getData()));
            message = sharedConsumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            log.info("Receive txn shared msg: {}", new String(message.getData(), UTF_8));
        }

        exclusiveConsumer.close();
        sharedConsumer.close();
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
            builder.setSequenceId(10L);
            builder.setTxnidMostBits(txnID.getMostSigBits());
            builder.setTxnidLeastBits(txnID.getLeastSigBits());
            builder.setPublishTime(System.currentTimeMillis());

            ByteBuf headerAndPayload = Commands.serializeMetadataAndPayload(
                    Commands.ChecksumType.Crc32c, builder.build(),
                    Unpooled.copiedBuffer((TXN_MSG_CONTENT + i).getBytes(UTF_8)));
            tb.appendBufferToTxn(txnID, i, headerAndPayload);
        }
        log.info("append messages to TB finish.");
    }

}
