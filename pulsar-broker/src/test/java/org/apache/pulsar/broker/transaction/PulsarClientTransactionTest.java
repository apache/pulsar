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

import com.google.common.collect.Sets;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.transaction.buffer.impl.PersistentTransactionBuffer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class PulsarClientTransactionTest extends BrokerTestBase {

    private static final String CLUSTER_NAME = "test";
    private static final String TENANT = "tnx";
    private static final String NAMESPACE1 = TENANT + "/ns1";
    private static final String TOPIC_INPUT_1 = NAMESPACE1 + "/input1";
    private static final String TOPIC_INPUT_2 = NAMESPACE1 + "/input2";
    private static final String TOPIC_OUTPUT_1 = NAMESPACE1 + "/output1";
    private static final String TOPIC_OUTPUT_2 = NAMESPACE1 + "/output2";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        init();

        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(TOPIC_INPUT_1, 1);
        admin.topics().createPartitionedTopic(TOPIC_INPUT_2, 3);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT_1, 1);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT_2, 3);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        lookupUrl = new URI(brokerUrl.toString());
        if (isTcpLookup) {
            lookupUrl = new URI(pulsar.getBrokerServiceUrl());
        }
        pulsarClient = newPulsarClient(lookupUrl.toString(), 0);

        Thread.sleep(1000 * 3);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void produceCommitTest() throws Exception {
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Transaction tnx = pulsarClientImpl.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        long txnIdMostBits = ((TransactionImpl) tnx).getTxnIdMostBits();
        long txnIdLeastBits = ((TransactionImpl) tnx).getTxnIdLeastBits();
        Assert.assertTrue(txnIdMostBits > -1);
        Assert.assertTrue(txnIdLeastBits > -1);

        PartitionedProducerImpl<byte[]> outProducer = (PartitionedProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(TOPIC_OUTPUT_1)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        int messageCnt = 5;
        String content = "Hello Txn - ";
        for (int i = 0; i < messageCnt; i++) {
            CompletableFuture<MessageId> produceFuture = outProducer
                    .newMessage(tnx).value((content + i).getBytes(UTF_8)).sendAsync();
        }

        // TODO wait a moment for adding publish partition to transaction, need to be fixed
        Thread.sleep(1000 * 10);

//        ReadOnlyCursor originTopicCursor = getOriginTopicCursor(TOPIC_OUTPUT_1, 0);
//        Assert.assertNotNull(originTopicCursor);

        ReadOnlyCursor tbTopicCursor = getTBTopicCursor(TOPIC_OUTPUT_1, 0).get();
        Assert.assertNotNull(tbTopicCursor);

//        List<Entry> entries = originTopicCursor.readEntries(10);
//        Assert.assertEquals(0, entries.size());

        tnx.commit().get();

        List<Entry> entries = tbTopicCursor.readEntries(10);
        for (int i = 0; i < messageCnt; i++) {
            PulsarApi.MessageMetadata messageMetadata = Commands.parseMessageMetadata(entries.get(i).getDataBuffer());
            Assert.assertEquals(messageMetadata.getTxnidMostBits(), txnIdMostBits);
            Assert.assertEquals(messageMetadata.getTxnidLeastBits(), txnIdLeastBits);

            byte[] bytes = new byte[entries.get(i).getDataBuffer().readableBytes()];
            entries.get(i).getDataBuffer().readBytes(bytes);
            System.out.println(new String(bytes));
            Assert.assertEquals(new String(bytes), content + i);
        }

//        entries = originTopicCursor.readEntries(10);
//        Assert.assertEquals(1, entries.size());

//        CountDownLatch countDownLatch = new CountDownLatch(5);
//        messageIdFutureList.forEach(messageIdFuture -> {
//            messageIdFuture.whenComplete((messageId, throwable) -> {
//                countDownLatch.countDown();
//                if (throwable != null) {
//                    log.error("Tnx commit failed! tnx: " + tnx, throwable);
//                    Assert.fail("Tnx commit failed! tnx: " + tnx);
//                    return;
//                }
//                Assert.assertNotNull(messageId);
//                log.info("Tnx commit success! messageId: {}", messageId);
//            });
//        });
//        countDownLatch.await();
    }

    private CompletableFuture<ReadOnlyCursor> getTBTopicCursor(String topic, int partition) {
        CompletableFuture<ReadOnlyCursor> cursorFuture = new CompletableFuture<>();
        try {
            String tbTopicName = PersistentTransactionBuffer.getTransactionBufferTopicName(
                    TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition);

            pulsar.getManagedLedgerFactory().asyncOpenReadOnlyCursor(
                    TopicName.get(tbTopicName).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig(), new AsyncCallbacks.OpenReadOnlyCursorCallback() {
                        @Override
                        public void openReadOnlyCursorComplete(ReadOnlyCursor cursor, Object ctx) {
                            cursorFuture.complete(cursor);
                        }

                        @Override
                        public void openReadOnlyCursorFailed(ManagedLedgerException exception, Object ctx) {
                            cursorFuture.completeExceptionally(exception);
                        }
                    }, null);
        } catch (Exception e) {
            e.printStackTrace();
            cursorFuture.completeExceptionally(e);
        }
        return cursorFuture;
    }

    private ReadOnlyCursor getOriginTopicCursor(String topic, int partition) {
        try {
            String partitionTopic = TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
            ReadOnlyCursor readOnlyCursor = pulsar.getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(partitionTopic).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig());
            return readOnlyCursor;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

//    @Test
    public void clientTest() throws Exception {
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Transaction tnx = pulsarClientImpl.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        String input1 = NAMESPACE1 + "/input1";
        String input2 = NAMESPACE1 + "/input2";
        String output = NAMESPACE1 + "/output";

        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClientImpl
                .newConsumer()
                .topic(input1)
                .subscriptionName("test")
                .subscribe();

        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClientImpl
                .newConsumer()
                .topic(input2)
                .subscriptionName("test")
                .subscribe();

        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(input1)
                .create();

        producer1.newMessage().value("Hello Tnx1".getBytes()).send();

        ProducerImpl<byte[]> producer2 = (ProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(input2)
                .create();

        producer2.newMessage().value("Hello Tnx1".getBytes()).send();

        Message<byte[]> inComingMsg1 = consumer1.receive();
        CompletableFuture<Void> ackFuture1 = consumer1.acknowledgeAsync(inComingMsg1.getMessageId(), tnx);

        Message<byte[]> inComingMsg2 = consumer2.receive();
        CompletableFuture<Void> ackFuture2 = consumer2.acknowledgeAsync(inComingMsg2.getMessageId(), tnx);

        ProducerImpl<byte[]> outProducer = (ProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(output)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        CompletableFuture<MessageId> outFuture1 = outProducer
                .newMessage(tnx).value(inComingMsg1.getData()).sendAsync();

        CompletableFuture<MessageId> outFuture2 = outProducer
                .newMessage(tnx).value(inComingMsg2.getData()).sendAsync();

        tnx.abort().get();

        ackFuture1.whenComplete((i, t) -> log.info("finish ack1"));
        ackFuture2.whenComplete((i, t) -> log.info("finish ack2"));
        outFuture1.whenComplete((id, t) -> log.info("finish out1 msgId: {}", id));
        outFuture2.whenComplete((id, t) -> log.info("finish out2 msgId: {}", id));
    }

}
