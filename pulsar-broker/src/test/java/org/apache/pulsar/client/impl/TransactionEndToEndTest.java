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
package org.apache.pulsar.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoop;
import io.netty.util.concurrent.EventExecutor;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * End to end transaction test.
 */
@Slf4j
@Test(groups = "flaky")
public class TransactionEndToEndTest extends TransactionTestBase {

    protected static final int TOPIC_PARTITION = 3;
    protected static final String TOPIC_OUTPUT = NAMESPACE1 + "/output";
    protected static final String TOPIC_MESSAGE_ACK_TEST = NAMESPACE1 + "/message-ack-test";
    protected static final int NUM_PARTITIONS = 16;
    @BeforeMethod
    protected void setup() throws Exception {
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);
        setUpBase(1, NUM_PARTITIONS, TOPIC_OUTPUT, TOPIC_PARTITION);
        admin.topics().createPartitionedTopic(TOPIC_MESSAGE_ACK_TEST, 1);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @DataProvider(name = "enableBatch")
    public Object[][] enableBatch() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test
    private void testIndividualAckAbortFilterAckSetInPendingAckState() throws Exception {
        final String topicName = NAMESPACE1 + "/testIndividualAckAbortFilterAckSetInPendingAckState";
        final int count = 9;
        Producer<Integer> producer = pulsarClient
                .newProducer(Schema.INT32)
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .batchingMaxMessages(count).create();

        @Cleanup
        Consumer<Integer> consumer = pulsarClient
                .newConsumer(Schema.INT32)
                .topic(topicName)
                .isAckReceiptEnabled(true)
                .subscriptionName("test")
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        for (int i = 0; i < count; i++) {
            producer.sendAsync(i);
        }

        Transaction firstTransaction = getTxn();

        Transaction secondTransaction = getTxn();

        // firstTransaction ack the first three messages and don't end the firstTransaction
        for (int i = 0; i < count / 3; i++) {
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), firstTransaction).get();
        }

        // if secondTransaction abort we only can receive the middle three messages
        for (int i = 0; i < count / 3; i++) {
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), secondTransaction).get();
        }

        // consumer normal ack the last three messages
        for (int i = 0; i < count / 3; i++) {
            consumer.acknowledgeAsync(consumer.receive()).get();
        }

        // if secondTransaction abort we only can receive the middle three messages
        secondTransaction.abort().get();

        // can receive 3 4 5 bit sit message
        for (int i = 0; i < count / 3; i++) {
            assertEquals(consumer.receive().getValue().intValue(), i + 3);
        }

        // can't receive message anymore
        assertNull(consumer.receive(2, TimeUnit.SECONDS));
    }


    @Test(dataProvider="enableBatch")
    private void testFilterMsgsInPendingAckStateWhenConsumerDisconnect(boolean enableBatch) throws Exception {
        final String topicName = NAMESPACE1 + "/testFilterMsgsInPendingAckStateWhenConsumerDisconnect-" + enableBatch;
        final int count = 10;

        @Cleanup
        Producer<Integer> producer = null;
        if (enableBatch) {
            producer = pulsarClient
                    .newProducer(Schema.INT32)
                    .topic(topicName)
                    .enableBatching(true)
                    .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                    .batchingMaxMessages(count).create();
        } else {
            producer = pulsarClient
                    .newProducer(Schema.INT32)
                    .topic(topicName)
                    .enableBatching(false).create();
        }

        @Cleanup
        Consumer<Integer> consumer = pulsarClient
                .newConsumer(Schema.INT32)
                .topic(topicName)
                .isAckReceiptEnabled(true)
                .subscriptionName("test")
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        for (int i = 0; i < count; i++) {
            producer.sendAsync(i);
        }

        Transaction txn1 = getTxn();

        Transaction txn2 = getTxn();


        // txn1 ack half of messages and don't end the txn1
        for (int i = 0; i < count / 2; i++) {
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), txn1).get();
        }

        // txn2 ack the rest half of messages and commit tnx2
        for (int i = count / 2; i < count; i++) {
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), txn2).get();
        }
        // commit txn2
        txn2.commit().get();

        // close and re-create consumer
        consumer.close();
        consumer = pulsarClient
                .newConsumer(Schema.INT32)
                .topic(topicName)
                .isAckReceiptEnabled(true)
                .subscriptionName("test")
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        Message<Integer> message = consumer.receive(3, TimeUnit.SECONDS);
        Assert.assertNull(message);

        // abort txn1
        txn1.abort().get();
        // after txn1 aborted, consumer will receive messages txn1 contains
        int receiveCounter = 0;
        while((message = consumer.receive(3, TimeUnit.SECONDS)) != null) {
            Assert.assertEquals(message.getValue().intValue(), receiveCounter);
            receiveCounter ++;
        }
        Assert.assertEquals(receiveCounter, count / 2);
    }

    @Test(dataProvider="enableBatch")
    private void produceCommitTest(boolean enableBatch) throws Exception {
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(TOPIC_OUTPUT)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        ProducerBuilder<byte[]> producerBuilder = pulsarClient
                .newProducer()
                .topic(TOPIC_OUTPUT)
                .enableBatching(enableBatch)
                .sendTimeout(0, TimeUnit.SECONDS);
        @Cleanup
        Producer<byte[]> producer = producerBuilder.create();

        Transaction txn1 = getTxn();
        Transaction txn2 = getTxn();

        int txnMessageCnt = 0;
        int messageCnt = 1000;
        for (int i = 0; i < messageCnt; i++) {
            if (i % 5 == 0) {
                producer.newMessage(txn1).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
            } else {
                producer.newMessage(txn2).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
            }
            txnMessageCnt++;
        }

        // Can't receive transaction messages before commit.
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        txn1.commit().get();
        txn2.commit().get();

        int receiveCnt = 0;
        for (int i = 0; i < txnMessageCnt; i++) {
            message = consumer.receive();
            Assert.assertNotNull(message);
            receiveCnt ++;
        }
        Assert.assertEquals(txnMessageCnt, receiveCnt);

        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        log.info("message commit test enableBatch {}", enableBatch);
    }

    @Test
    public void produceAbortTest() throws Exception {
        Transaction txn = getTxn();
        String subName = "test";

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC_OUTPUT)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        int messageCnt = 10;
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage(txn).value(("Hello Txn - " + i).getBytes(UTF_8)).send();
        }

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(TOPIC_OUTPUT)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        // Can't receive transaction messages before abort.
        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);

        txn.abort().get();

        // Cant't receive transaction messages after abort.
        message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);
        Awaitility.await().until(() -> {
            boolean flag = true;
            for (int partition = 0; partition < TOPIC_PARTITION; partition ++) {
                String topic;
                topic = TopicName.get(TOPIC_OUTPUT).getPartition(partition).toString();
                boolean exist = false;
                for (int i = 0; i < getPulsarServiceList().size(); i++) {

                    Field field = BrokerService.class.getDeclaredField("topics");
                    field.setAccessible(true);
                    ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                            (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                                    .get(getPulsarServiceList().get(i).getBrokerService());
                    CompletableFuture<Optional<Topic>> topicFuture = topics.get(topic);

                    if (topicFuture != null) {
                        Optional<Topic> topicOptional = topicFuture.get();
                        if (topicOptional.isPresent()) {
                            PersistentSubscription persistentSubscription =
                                    (PersistentSubscription) topicOptional.get().getSubscription(subName);
                            Position markDeletePosition = persistentSubscription.getCursor().getMarkDeletedPosition();
                            Position lastConfirmedEntry = persistentSubscription.getCursor()
                                    .getManagedLedger().getLastConfirmedEntry();
                            exist = true;
                            if (!markDeletePosition.equals(lastConfirmedEntry)) {
                                //this because of the transaction commit marker have't delete
                                //delete commit marker after ack position
                                //when delete commit marker operation is processing, next delete operation will not do again
                                //when delete commit marker operation finish, it can run next delete commit marker operation
                                //so this test may not delete all the position in this manageLedger.
                                Position markerPosition = ((ManagedLedgerImpl) persistentSubscription.getCursor()
                                        .getManagedLedger()).getNextValidPosition((PositionImpl) markDeletePosition);
                                //marker is the lastConfirmedEntry, after commit the marker will only be write in
                                if (!markerPosition.equals(lastConfirmedEntry)) {
                                    log.error("Mark delete position is not commit marker position!");
                                    flag = false;
                                }
                            }
                        }
                    }
                }
                assertTrue(exist);
            }
            return flag;
        });

        log.info("finished test partitionAbortTest");
    }

    @Test(dataProvider="enableBatch")
    private void testAckWithTransactionReduceUnAckMessageCount(boolean enableBatch) throws Exception {

        final int messageCount = 50;
        final String subName = "testAckWithTransactionReduceUnAckMessageCount";
        final String topicName = NAMESPACE1 + "/testAckWithTransactionReduceUnAckMessageCount-" + enableBatch;
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(true)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topicName)
                .enableBatching(enableBatch)
                .batchingMaxMessages(10)
                .create();

        CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            producer.sendAsync((i + "").getBytes()).thenRun(countDownLatch::countDown);
        }

        countDownLatch.await();

        Transaction txn = getTxn();

        for (int i = 0; i < messageCount / 2; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledgeAsync(message.getMessageId(), txn).get();
        }

        txn.commit().get();
        boolean flag = false;
        String topic = TopicName.get(topicName).toString();
        for (int i = 0; i < getPulsarServiceList().size(); i++) {
            CompletableFuture<Optional<Topic>> topicFuture = getPulsarServiceList().get(i)
                    .getBrokerService().getTopic(topic, false);

            if (topicFuture != null) {
                Optional<Topic> topicOptional = topicFuture.get();
                if (topicOptional.isPresent()) {
                    PersistentSubscription persistentSubscription =
                            (PersistentSubscription) topicOptional.get().getSubscription(subName);
                    assertEquals(persistentSubscription.getConsumers().get(0).getUnackedMessages(), messageCount / 2);
                    flag = true;
                }
            }
        }
        assertTrue(flag);
    }

    @Test
    public void txnIndividualAckTestNoBatchAndSharedSub() throws Exception {
        txnAckTest(false, 1, SubscriptionType.Shared);
    }

    @Test
    public void txnIndividualAckTestBatchAndSharedSub() throws Exception {
        txnAckTest(true, 200, SubscriptionType.Shared);
    }

    @Test
    public void txnIndividualAckTestNoBatchAndFailoverSub() throws Exception {
        txnAckTest(false, 1, SubscriptionType.Failover);
    }

    @Test
    public void txnIndividualAckTestBatchAndFailoverSub() throws Exception {
        txnAckTest(true, 200, SubscriptionType.Failover);
    }

    protected void txnAckTest(boolean batchEnable, int maxBatchSize,
                         SubscriptionType subscriptionType) throws Exception {
        String normalTopic = NAMESPACE1 + "/normal-topic";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(normalTopic)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(subscriptionType)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(normalTopic)
                .enableBatching(batchEnable)
                .batchingMaxMessages(maxBatchSize)
                .create();

        for (int retryCnt = 0; retryCnt < 2; retryCnt++) {
            Transaction txn = getTxn();

            int messageCnt = 1000;
            // produce normal messages
            for (int i = 0; i < messageCnt; i++){
                producer.newMessage().value("hello".getBytes()).sendAsync();
            }

            // consume and ack messages with txn
            for (int i = 0; i < messageCnt; i++) {
                Message<byte[]> message = consumer.receive();
                Assert.assertNotNull(message);
                log.info("receive msgId: {}, count : {}", message.getMessageId(), i);
                consumer.acknowledgeAsync(message.getMessageId(), txn).get();
            }

            // the messages are pending ack state and can't be received
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);

            // 1) txn abort
            txn.abort().get();

            // after transaction abort, the messages could be received
            Transaction commitTxn = getTxn();
            for (int i = 0; i < messageCnt; i++) {
                message = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                consumer.acknowledgeAsync(message.getMessageId(), commitTxn).get();
                log.info("receive msgId: {}, count: {}", message.getMessageId(), i);
            }

            // 2) ack committed by a new txn
            commitTxn.commit().get();

            // after transaction commit, the messages can't be received
            message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);

            Field field = TransactionImpl.class.getDeclaredField("state");
            field.setAccessible(true);
            field.set(commitTxn, TransactionImpl.State.OPEN);
            try {
                commitTxn.commit().get();
                fail("recommit one transaction should be failed.");
            } catch (Exception reCommitError) {
                // recommit one transaction should be failed
                log.info("expected exception for recommit one transaction.");
                Assert.assertNotNull(reCommitError);
                Assert.assertTrue(reCommitError.getCause() instanceof TransactionNotFoundException);
            }
        }
    }

    @Test
    public void testAfterDeleteTopicOtherTopicCanRecover() throws Exception {
        String topicOne = "persistent://" + NAMESPACE1 + "/topic-one";
        String topicTwo = "persistent://" + NAMESPACE1 + "/topic-two";
        String sub = "test";
        admin.topics().createNonPartitionedTopic(topicOne);
        admin.topics().createSubscription(topicOne, "test", MessageId.earliest);
        admin.topics().delete(topicOne);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicTwo).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicTwo).subscriptionName(sub).subscribe();
        String content = "test";
        producer.send(content);
        assertEquals(consumer.receive().getValue(), content);
    }

    @Test
    public void txnMessageAckTest() throws Exception {
        String topic = TOPIC_MESSAGE_ACK_TEST;
        final String subName = "test";
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Transaction txn = getTxn();

        int messageCnt = 10;
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage(txn).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
        }
        log.info("produce transaction messages finished");

        // Can't receive transaction messages before commit.
        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);
        log.info("transaction messages can't be received before transaction committed");

        txn.commit().get();

        int ackedMessageCount = 0;
        int receiveCnt = 0;
        for (int i = 0; i < messageCnt; i++) {
            message = consumer.receive();
            Assert.assertNotNull(message);
            receiveCnt ++;
            if (i % 2 == 0) {
                consumer.acknowledge(message);
                ackedMessageCount ++;
            }
        }
        Assert.assertEquals(messageCnt, receiveCnt);

        message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);

        String checkTopic = TopicName.get(topic).getPartition(0).toString();
        PersistentTopicInternalStats stats = admin.topics().getInternalStats(checkTopic, false);

        Assert.assertNotEquals(stats.cursors.get(subName).markDeletePosition, stats.lastConfirmedEntry);

        consumer.redeliverUnacknowledgedMessages();

        receiveCnt = 0;
        for (int i = 0; i < messageCnt - ackedMessageCount; i++) {
            message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            consumer.acknowledge(message);
            receiveCnt ++;
        }
        Assert.assertEquals(messageCnt - ackedMessageCount, receiveCnt);

        message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);

        topic = TopicName.get(topic).getPartition(0).toString();
        boolean exist = false;
        for (int i = 0; i < getPulsarServiceList().size(); i++) {

            Field field = BrokerService.class.getDeclaredField("topics");
            field.setAccessible(true);
            ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                    (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                            .get(getPulsarServiceList().get(i).getBrokerService());
            CompletableFuture<Optional<Topic>> topicFuture = topics.get(topic);

            if (topicFuture != null) {
                Optional<Topic> topicOptional = topicFuture.get();
                if (topicOptional.isPresent()) {
                    PersistentSubscription persistentSubscription =
                            (PersistentSubscription) topicOptional.get().getSubscription(subName);
                    Position markDeletePosition = persistentSubscription.getCursor().getMarkDeletedPosition();
                    Position lastConfirmedEntry = persistentSubscription.getCursor()
                            .getManagedLedger().getLastConfirmedEntry();
                    exist = true;
                    if (!markDeletePosition.equals(lastConfirmedEntry)) {
                        //this because of the transaction commit marker have't delete
                        //delete commit marker after ack position
                        //when delete commit marker operation is processing, next delete operation will not do again
                        //when delete commit marker operation finish, it can run next delete commit marker operation
                        //so this test may not delete all the position in this manageLedger.
                        Position markerPosition = ((ManagedLedgerImpl) persistentSubscription.getCursor()
                                .getManagedLedger()).getNextValidPosition((PositionImpl) markDeletePosition);
                        //marker is the lastConfirmedEntry, after commit the marker will only be write in
                        if (!markerPosition.equals(lastConfirmedEntry)) {
                            log.error("Mark delete position is not commit marker position!");
                            fail();
                        }
                    }
                }
            }
        }
        assertTrue(exist);

        log.info("receive transaction messages count: {}", receiveCnt);
    }

    @Test
    public void txnAckTestBatchAndCumulativeSub() throws Exception {
        txnCumulativeAckTest(true, 200, SubscriptionType.Failover);
    }

    @Test
    public void txnAckTestNoBatchAndCumulativeSub() throws Exception {
        txnCumulativeAckTest(false, 1, SubscriptionType.Failover);
    }

    private void txnCumulativeAckTest(boolean batchEnable, int maxBatchSize, SubscriptionType subscriptionType)
            throws Exception {
        String normalTopic = NAMESPACE1 + "/normal-topic";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(normalTopic)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(subscriptionType)
                .ackTimeout(1, TimeUnit.MINUTES)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(normalTopic)
                .enableBatching(batchEnable)
                .batchingMaxMessages(maxBatchSize)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        for (int retryCnt = 0; retryCnt < 2; retryCnt++) {
            Transaction abortTxn = getTxn();
            int messageCnt = 1000;
            // produce normal messages
            for (int i = 0; i < messageCnt; i++){
                producer.newMessage().value("hello".getBytes()).sendAsync();
            }
            Message<byte[]> message = null;
            Thread.sleep(1000L);
            for (int i = 0; i < messageCnt; i++) {
                message = consumer.receive(1, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                if (i % 3 == 0) {
                    consumer.acknowledgeCumulativeAsync(message.getMessageId(), abortTxn).get();
                }
                log.info("receive msgId abort: {}, retryCount : {}, count : {}", message.getMessageId(), retryCnt, i);
            }
            try {
                consumer.acknowledgeCumulativeAsync(message.getMessageId(), abortTxn).get();
                fail("not ack conflict ");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }

            try {
                consumer.acknowledgeCumulativeAsync(DefaultImplementation.getDefaultImplementation()
                        .newMessageId(((MessageIdImpl) message.getMessageId()).getLedgerId(),
                                ((MessageIdImpl) message.getMessageId()).getEntryId() - 1, -1),
                        abortTxn).get();
                fail("not ack conflict ");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }

            // the messages are pending ack state and can't be received
            message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);

            abortTxn.abort().get();
            Transaction commitTxn = getTxn();
            for (int i = 0; i < messageCnt; i++) {
                message = consumer.receive(1, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                if (i % 3 == 0) {
                    consumer.acknowledgeCumulativeAsync(message.getMessageId(), commitTxn).get();
                }
                log.info("receive msgId abort: {}, retryCount : {}, count : {}", message.getMessageId(), retryCnt, i);
            }

            commitTxn.commit().get();
            Field field = TransactionImpl.class.getDeclaredField("state");
            field.setAccessible(true);
            field.set(commitTxn, TransactionImpl.State.OPEN);
            try {
                commitTxn.commit().get();
                fail("recommit one transaction should be failed.");
            } catch (Exception reCommitError) {
                // recommit one transaction should be failed
                log.info("expected exception for recommit one transaction.");
                Assert.assertNotNull(reCommitError);
                Assert.assertTrue(reCommitError.getCause() instanceof TransactionNotFoundException);
            }

            message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }
    }

    public Transaction getTxn() throws Exception {
        return pulsarClient
                .newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS)
                .build()
                .get();
    }

    private void markDeletePositionCheck(String topic, String subName, boolean equalsWithLastConfirm) throws Exception {
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            PersistentTopicInternalStats stats = null;
            String checkTopic = TopicName.get(topic).getPartition(i).toString();
            for (int j = 0; j < 10; j++) {
                stats = admin.topics().getInternalStats(checkTopic, false);
                if (stats.lastConfirmedEntry.equals(stats.cursors.get(subName).markDeletePosition)) {
                    break;
                }
                Thread.sleep(200);
            }
            if (equalsWithLastConfirm) {
                Assert.assertEquals(stats.cursors.get(subName).markDeletePosition, stats.lastConfirmedEntry);
            } else {
                Assert.assertNotEquals(stats.cursors.get(subName).markDeletePosition, stats.lastConfirmedEntry);
            }
        }
    }

    @Test
    public void txnMetadataHandlerRecoverTest() throws Exception {
        String topic = NAMESPACE1 + "/tc-metadata-handler-recover";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        List<TxnID> txnIDList = new ArrayList<>();

        int txnCnt = 20;
        int messageCnt = 10;
        for (int i = 0; i < txnCnt; i++) {
            TransactionImpl txn = (TransactionImpl) pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.MINUTES)
                    .build().get();
            for (int j = 0; j < messageCnt; j++) {
                producer.newMessage(txn).value("Hello".getBytes()).sendAsync().get();
            }
            txnIDList.add(new TxnID(txn.getTxnIdMostBits(), txn.getTxnIdLeastBits()));
        }

        @Cleanup
        PulsarClientImpl recoverPulsarClient = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        TransactionCoordinatorClient tcClient = recoverPulsarClient.getTcClient();
        for (TxnID txnID : txnIDList) {
            tcClient.commit(txnID);
        }

        @Cleanup
        Consumer<byte[]> consumer = recoverPulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        for (int i = 0; i < txnCnt * messageCnt; i++) {
            Message<byte[]> message = consumer.receive();
            Assert.assertNotNull(message);
        }
    }

    @Test
    public void produceTxnMessageOrderTest() throws Exception {
        String topic = NAMESPACE1 + "/txn-produce-order";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .producerName("txn-publish-order")
                .create();

        for (int ti = 0; ti < 10; ti++) {
            Transaction txn = pulsarClient
                    .newTransaction()
                    .withTransactionTimeout(2, TimeUnit.SECONDS)
                    .build().get();

            for (int i = 0; i < 1000; i++) {
                producer.newMessage(txn).value(("" + i).getBytes()).sendAsync();
            }
            txn.commit().get();

            for (int i = 0; i < 1000; i++) {
                Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                Assert.assertEquals(Integer.valueOf(new String(message.getData())), new Integer(i));
            }
        }
    }

    @Test
    public void produceAndConsumeCloseStateTxnTest() throws Exception {
        String topic = NAMESPACE1 + "/txn-close-state";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .producerName("txn-close-state")
                .create();

        Transaction produceTxn = pulsarClient
                .newTransaction()
                .withTransactionTimeout(2, TimeUnit.SECONDS)
                .build().get();

        Transaction consumeTxn = pulsarClient
                .newTransaction()
                .withTransactionTimeout(2, TimeUnit.SECONDS)
                .build().get();

        producer.newMessage(produceTxn).value(("Hello Pulsar!").getBytes()).sendAsync().get();
        produceTxn.commit().get();
        try {
            producer.newMessage(produceTxn).value(("Hello Pulsar!").getBytes()).sendAsync().get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.InvalidTxnStatusException);
        }

        try {
            produceTxn.commit().get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.InvalidTxnStatusException);
        }


        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message.getMessageId(), consumeTxn).get();
        consumeTxn.commit().get();
        try {
            consumer.acknowledgeAsync(message.getMessageId(), consumeTxn).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.InvalidTxnStatusException);
        }

        try {
            consumeTxn.commit().get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.InvalidTxnStatusException);
        }

        Transaction timeoutTxn = pulsarClient
                .newTransaction()
                .withTransactionTimeout(1, TimeUnit.SECONDS)
                .build().get();
        AtomicReference<TransactionMetadataStore> transactionMetadataStore = new AtomicReference<>();
        getPulsarServiceList().forEach(pulsarService -> {
            if (pulsarService.getTransactionMetadataStoreService().getStores()
                    .containsKey(TransactionCoordinatorID.get(((TransactionImpl) timeoutTxn).getTxnIdMostBits()))) {
                transactionMetadataStore.set(pulsarService.getTransactionMetadataStoreService().getStores()
                        .get(TransactionCoordinatorID.get(((TransactionImpl) timeoutTxn).getTxnIdMostBits())));
            }
        });

        Awaitility.await().until(() -> {
            try {
                transactionMetadataStore.get().getTxnMeta(new TxnID(((TransactionImpl) timeoutTxn)
                        .getTxnIdMostBits(), ((TransactionImpl) timeoutTxn).getTxnIdLeastBits())).get();
                return false;
            } catch (Exception e) {
                return true;
            }
        });

        Class<TransactionImpl> transactionClass = TransactionImpl.class;
        Constructor<TransactionImpl> constructor = transactionClass
                .getDeclaredConstructor(PulsarClientImpl.class, long.class, long.class, long.class);
        constructor.setAccessible(true);

        TransactionImpl timeoutTxnSkipClientTimeout = constructor.newInstance(pulsarClient, 5,
                        timeoutTxn.getTxnID().getLeastSigBits(), timeoutTxn.getTxnID().getMostSigBits());

        try {
            timeoutTxnSkipClientTimeout.commit().get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionNotFoundException);
        }
        Field field = TransactionImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        TransactionImpl.State state = (TransactionImpl.State) field.get(timeoutTxnSkipClientTimeout);
        assertEquals(state, TransactionImpl.State.ERROR);
    }

    @Test
    public void testTxnTimeoutAtTransactionMetadataStore() throws Exception{
        TxnID txnID = pulsarServiceList.get(0).getTransactionMetadataStoreService()
                .newTransaction(new TransactionCoordinatorID(0), 1, null).get();
        Awaitility.await().until(() -> {
            try {
               getPulsarServiceList().get(0).getTransactionMetadataStoreService().getTxnMeta(txnID).get();
                return false;
            } catch (Exception e) {
                return true;
            }
        });
        Collection<TransactionMetadataStore> transactionMetadataStores =
                getPulsarServiceList().get(0).getTransactionMetadataStoreService().getStores().values();
        long timeoutCount = transactionMetadataStores.stream()
                .mapToLong(store -> store.getMetadataStoreStats().timeoutCount).sum();
        Assert.assertEquals(timeoutCount, 1);
    }

    @Test
    public void transactionTimeoutTest() throws Exception {
        String topic = NAMESPACE1 + "/txn-timeout";

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .producerName("txn-timeout")
                .create();

        producer.send("Hello Pulsar!");

        Transaction consumeTimeoutTxn = pulsarClient
                .newTransaction()
                .withTransactionTimeout(3, TimeUnit.SECONDS)
                .build().get();

        Message<String> message = consumer.receive();

        consumer.acknowledgeAsync(message.getMessageId(), consumeTimeoutTxn).get();

        Message<String> reReceiveMessage = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(reReceiveMessage);

        reReceiveMessage = consumer.receive(2, TimeUnit.SECONDS);

        assertEquals(reReceiveMessage.getValue(), message.getValue());

        assertEquals(reReceiveMessage.getMessageId(), message.getMessageId());

    }

    @DataProvider(name = "ackType")
    public static Object[] ackType() {
        return new Object[] {CommandAck.AckType.Cumulative, CommandAck.AckType.Individual};
    }

    @Test(dataProvider = "ackType")
    public void txnTransactionRedeliverNullDispatcher(CommandAck.AckType ackType) throws Exception {
        String topic = NAMESPACE1 + "/txnTransactionRedeliverNullDispatcher";
        final String subName = "test";
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();


        int messageCnt = 10;
        for (int i = 0; i < messageCnt; i++) {
            producer.send(("Hello Txn - " + i).getBytes(UTF_8));
        }
        Transaction txn = getTxn();
        if (ackType == CommandAck.AckType.Individual) {
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), txn);
        } else {
            consumer.acknowledgeCumulativeAsync(consumer.receive().getMessageId(), txn);
        }
        topic = TopicName.get(topic).toString();
        boolean exist = false;
        for (int i = 0; i < getPulsarServiceList().size(); i++) {

            Field field = BrokerService.class.getDeclaredField("topics");
            field.setAccessible(true);
            ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                    (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                            .get(getPulsarServiceList().get(i).getBrokerService());
            CompletableFuture<Optional<Topic>> topicFuture = topics.get(topic);

            if (topicFuture != null) {
                Optional<Topic> topicOptional = topicFuture.get();
                if (topicOptional.isPresent()) {
                    PersistentSubscription persistentSubscription =
                            (PersistentSubscription) topicOptional.get().getSubscription(subName);
                    field = persistentSubscription.getClass().getDeclaredField("dispatcher");
                    field.setAccessible(true);
                    field.set(persistentSubscription, null);
                    exist = true;
                }
            }
        }
        txn.abort().get();
        assertTrue(exist);
    }

    @Test
    public void oneTransactionOneTopicWithMultiSubTest() throws Exception {
        String topic = NAMESPACE1 + "/oneTransactionOneTopicWithMultiSubTest";
        final String subName1 = "test1";
        final String subName2 = "test2";
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient
                .newConsumer()
                .topic(topic)
                .subscriptionName(subName1)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();
        Awaitility.await().until(consumer1::isConnected);

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient
                .newConsumer()
                .topic(topic)
                .subscriptionName(subName2)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();
        Awaitility.await().until(consumer2::isConnected);

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        MessageId messageId = producer.send(("Hello Pulsar").getBytes(UTF_8));
        TransactionImpl txn = (TransactionImpl) getTxn();
        consumer1.acknowledgeAsync(messageId, txn).get();
        consumer2.acknowledgeAsync(messageId, txn).get();

        boolean flag = false;
        for (int i = 0; i < getPulsarServiceList().size(); i++) {
            TransactionMetadataStoreService transactionMetadataStoreService =
                    getPulsarServiceList().get(i).getTransactionMetadataStoreService();
            if (transactionMetadataStoreService.getStores()
                    .containsKey(TransactionCoordinatorID.get(txn.getTxnIdMostBits()))) {
                List<TransactionSubscription> list = transactionMetadataStoreService
                        .getTxnMeta(new TxnID(txn.getTxnIdMostBits(), txn.getTxnIdLeastBits())).get().ackedPartitions();
                flag = true;
                assertEquals(list.size(), 2);
                if (list.get(0).getSubscription().equals(subName1)) {
                    assertEquals(list.get(1).getSubscription(), subName2);
                } else {
                    assertEquals(list.get(0).getSubscription(), subName2);
                    assertEquals(list.get(1).getSubscription(), subName1);
                }
            }
        }
        assertTrue(flag);
    }

    @Test
    public void testTxnTimeOutInClient() throws Exception{
        String topic = NAMESPACE1 + "/testTxnTimeOutInClient";
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).producerName("testTxnTimeOut_producer")
                .topic(topic).sendTimeout(0, TimeUnit.SECONDS).enableBatching(false).create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).consumerName("testTxnTimeOut_consumer")
                .topic(topic).subscriptionName("testTxnTimeOut_sub").subscribe();

        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.SECONDS)
                .build().get();
        producer.newMessage().send();
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(((TransactionImpl)transaction).getState(), TransactionImpl.State.TIME_OUT);
        });

        try {
            producer.newMessage(transaction).send();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getCause() instanceof TransactionCoordinatorClientException
                    .InvalidTxnStatusException);
        }
        try {
            Message<String> message = consumer.receive();
            consumer.acknowledgeAsync(message.getMessageId(), transaction).get();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof TransactionCoordinatorClientException
                    .InvalidTxnStatusException);
        }
    }

    @Test
    public void testSendTxnMessageTimeout() throws Exception {
        String topic = NAMESPACE1 + "/testSendTxnMessageTimeout";
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(1, TimeUnit.SECONDS)
                .create();

        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        // mock cnx, send message can't receive response
        ClientCnx cnx = mock(ClientCnx.class);
        Channel channel = mock(Channel.class);
        doReturn(spy(DefaultEventLoop.class)).when(channel).eventLoop();
        ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
        doReturn(channel).when(channelHandlerContext).channel();
        doReturn(channelHandlerContext).when(cnx).ctx();
        EventExecutor eventExecutor = mock(EventExecutor.class);
        doReturn(eventExecutor).when(channelHandlerContext).executor();
        CompletableFuture<ProducerResponse> completableFuture = new CompletableFuture<>();
        completableFuture.complete(new ProducerResponse("test", 1,
                "1".getBytes(), Optional.of(30L)));
        doReturn(completableFuture).when(cnx).sendRequestWithId(anyObject(), anyLong());
        producer.getConnectionHandler().setClientCnx(cnx);


        try {
            // send message with txn use mock cnx, will not receive send response
            producer.newMessage(transaction).value("Hello Pulsar!".getBytes()).send();
            fail();
        } catch (PulsarClientException ex) {
            assertTrue(ex instanceof PulsarClientException.TimeoutException);
        }
    }

    @Test
    public void testAckWithTransactionReduceUnackCountNotInPendingAcks() throws Exception {
        final String topic = "persistent://" + NAMESPACE1 + "/testAckWithTransactionReduceUnackCountNotInPendingAcks";
        final String subName = "test";
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .sendTimeout(1, TimeUnit.SECONDS)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName)
                .subscribe();

        // send 5 messages with one batch
        for (int i = 0; i < 5; i++) {
            producer.sendAsync((i + "").getBytes(UTF_8));
        }

        List<MessageId> messageIds = new ArrayList<>();

        // receive the batch messages add to a list
        for (int i = 0; i < 5; i++) {
            messageIds.add(consumer.receive().getMessageId());
        }

        MessageIdImpl messageId = (MessageIdImpl) messageIds.get(0);


        // remove the message from the pendingAcks, in fact redeliver will remove the messageId from the pendingAck
        getPulsarServiceList().get(0).getBrokerService().getTopic(topic, false)
                .get().get().getSubscription(subName).getConsumers().get(0).getPendingAcks()
                .remove(messageId.ledgerId, messageId.entryId);

        Transaction txn = getTxn();
        consumer.acknowledgeAsync(messageIds.get(1), txn).get();

        // ack one message, the unack count is 4
        assertEquals(getPulsarServiceList().get(0).getBrokerService().getTopic(topic, false)
                .get().get().getSubscription(subName).getConsumers().get(0).getUnackedMessages(), 4);
    }

    @Test
    public void testSendTxnAckMessageToDLQ() throws Exception {
        String topic = NAMESPACE1 + "/testSendTxnAckMessageToDLQ";
        String subName = "test";
        String value = "test";
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .sendTimeout(1, TimeUnit.SECONDS)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                // consumer can't receive the same message three times
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(1).build())
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Consumer<byte[]> deadLetterConsumer = pulsarClient.newConsumer()
                .topic(String.format("%s-%s" + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX,
                        topic, subName))
                .subscriptionType(SubscriptionType.Shared)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(1).build())
                .subscriptionName("test")
                .subscribe();

        producer.send(value.getBytes());
        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES)
                .build().get();

        // consumer receive the message the first time, redeliverCount = 0
        consumer.acknowledgeAsync(consumer.receive().getMessageId(), transaction).get();

        transaction.abort().get();

        transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES)
                .build().get();

        // consumer receive the message the second time, redeliverCount = 1, also can be received
        consumer.acknowledgeAsync(consumer.receive().getMessageId(), transaction).get();

        transaction.abort().get();

        // consumer receive the message the third time, redeliverCount = 2,
        // the message will be sent to DLQ, can't receive
        assertNull(consumer.receive(3, TimeUnit.SECONDS));

        assertEquals(((ConsumerImpl<?>) consumer).getAvailablePermits(), 3);

        assertEquals(value, new String(deadLetterConsumer.receive(3, TimeUnit.SECONDS).getValue()));
    }

    @Test
    public void testSendTxnAckBatchMessageToDLQ() throws Exception {
        String topic = NAMESPACE1 + "/testSendTxnAckBatchMessageToDLQ";
        String subName = "test";
        String value1 = "test1";
        String value2 = "test2";
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(1, TimeUnit.SECONDS)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                // consumer can't receive the same message three times
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(1).build())
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Consumer<byte[]> deadLetterConsumer = pulsarClient.newConsumer()
                .topic(String.format("%s-%s" + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX,
                        topic, subName))
                .subscriptionType(SubscriptionType.Shared)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(1).build())
                .subscriptionName("test")
                .subscribe();

        producer.sendAsync(value1.getBytes());
        producer.sendAsync(value2.getBytes());
        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES)
                .build().get();

        Message<byte[]> message = consumer.receive();
        assertEquals(value1, new String(message.getValue()));
        // consumer receive the batch message one the first time, redeliverCount = 0
        consumer.acknowledgeAsync(message.getMessageId(), transaction).get();

        transaction.abort().get();

        // consumer will receive the batch message two and then receive
        // the message one and message two again, redeliverCount = 1
        for (int i = 0; i < 3; i ++) {
            message = consumer.receive();
        }

        transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES)
                .build().get();

        assertEquals(value2, new String(message.getValue()));
        // consumer receive the batch message two the second time, redeliverCount = 1, also can be received
        consumer.acknowledgeAsync(message.getMessageId(), transaction).get();

        transaction.abort().get();

        // consumer receive the batch message the third time, redeliverCount = 2,
        // the message will be sent to DLQ, can't receive
        assertNull(consumer.receive(3, TimeUnit.SECONDS));

        assertEquals(((ConsumerImpl<?>) consumer).getAvailablePermits(), 6);

        assertEquals(value1, new String(deadLetterConsumer.receive(3, TimeUnit.SECONDS).getValue()));
        assertEquals(value2, new String(deadLetterConsumer.receive(3, TimeUnit.SECONDS).getValue()));
    }

    @Test
    public void testDelayedTransactionMessages() throws Exception {
        String topic = NAMESPACE1 + "/testDelayedTransactionMessages";

        @Cleanup
        Consumer<String> failoverConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("failover-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        @Cleanup
        Consumer<String> sharedConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();

        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
        for (int i = 0; i < 10; i++) {
            producer.newMessage(transaction)
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        transaction.commit().get();

        // Failover consumer will receive the messages immediately while
        // the shared consumer will get them after the delay
        Message<String> msg = sharedConsumer.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

        for (int i = 0; i < 10; i++) {
            msg = failoverConsumer.receive(100, TimeUnit.MILLISECONDS);
            assertEquals(msg.getValue(), "msg-" + i);
        }

        Set<String> receivedMsgs = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            msg = sharedConsumer.receive(10, TimeUnit.SECONDS);
            receivedMsgs.add(msg.getValue());
        }

        assertEquals(receivedMsgs.size(), 10);
        for (int i = 0; i < 10; i++) {
            assertTrue(receivedMsgs.contains("msg-" + i));
        }
    }
}
