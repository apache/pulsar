/*
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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.DEDUPLICATION_CURSOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicDuplicationTest extends ProducerConsumerBase {
    private final String testTenant = "my-property";
    private final String testNamespace = "my-ns";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/max-unacked-";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setBrokerDeduplicationEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    protected void resetDeduplicationStatus() throws Exception {
        admin.namespaces().removeDeduplicationStatus(myNamespace);
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 10000)
    public void testDuplicationApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        waitCacheInit(topicName);
        Boolean enabled = admin.topics().getDeduplicationEnabled(topicName);
        assertNull(enabled);

        admin.topics().enableDeduplication(topicName, true);
        Awaitility.await()
                .until(()-> admin.topics().getDeduplicationEnabled(topicName) != null);
        assertTrue(admin.topics().getDeduplicationEnabled(topicName));

        admin.topics().disableDeduplication(topicName);
        Awaitility.await()
                .until(()-> admin.topics().getMaxUnackedMessagesOnSubscription(topicName) == null);
        assertNull(admin.topics().getDeduplicationEnabled(topicName));
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 10000)
    public void testTopicDuplicationApi2() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        waitCacheInit(topicName);
        Boolean enabled = admin.topics().getDeduplicationStatus(topicName);
        assertNull(enabled);

        admin.topics().setDeduplicationStatus(topicName, true);
        Awaitility.await()
                .until(() -> admin.topics().getDeduplicationStatus(topicName) != null);
        assertTrue(admin.topics().getDeduplicationStatus(topicName));

        admin.topics().removeDeduplicationStatus(topicName);
        Awaitility.await()
                .until(() -> admin.topics().getMaxUnackedMessagesOnSubscription(topicName) == null);
        assertNull(admin.topics().getDeduplicationStatus(topicName));
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 10000)
    public void testTopicDuplicationAppliedApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        waitCacheInit(topicName);
        assertNull(admin.namespaces().getDeduplicationStatus(myNamespace));
        assertNull(admin.topics().getDeduplicationStatus(topicName));
        assertEquals(admin.topics().getDeduplicationStatus(topicName, true).booleanValue(),
                conf.isBrokerDeduplicationEnabled());

        admin.namespaces().setDeduplicationStatus(myNamespace, false);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.topics().getDeduplicationStatus(topicName, true)));
        admin.topics().setDeduplicationStatus(topicName, true);
        Awaitility.await().untilAsserted(() -> assertTrue(admin.topics().getDeduplicationStatus(topicName, true)));

        admin.topics().removeDeduplicationStatus(topicName);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.topics().getDeduplicationStatus(topicName, true)));
        admin.namespaces().removeDeduplicationStatus(myNamespace);
        Awaitility.await().untilAsserted(() -> assertEquals(admin.topics()
                        .getDeduplicationStatus(topicName, true).booleanValue(),
                conf.isBrokerDeduplicationEnabled()));
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 30000)
    public void testDeduplicationPriority() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        final int maxMsgNum = 5;
        waitCacheInit(topicName);
        //1) Start up producer and send msg.We specified the max sequenceId
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .producerName(producerName).create();
        long maxSeq = sendMessageAndGetMaxSeq(maxMsgNum, producer);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
        //broker-level deduplication is enabled in setup() by default
        checkDeduplicationEnabled(producerName, messageDeduplication, maxSeq);
        //disabled in namespace-level
        admin.namespaces().setDeduplicationStatus(myNamespace, false);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces()
                .getDeduplicationStatus(myNamespace)));
        sendMessageAndGetMaxSeq(maxMsgNum, producer);
        checkDeduplicationDisabled(producerName, messageDeduplication);
        //enabled in topic-level
        admin.topics().setDeduplicationStatus(topicName, true);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getDeduplicationStatus(topicName)));
        Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication.isEnabled()));
        long maxSeq2 = sendMessageAndGetMaxSeq(maxMsgNum, producer);
        checkDeduplicationEnabled(producerName, messageDeduplication, maxSeq2);
        //remove topic-level, use namespace-level
        admin.topics().removeDeduplicationStatus(topicName);
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getDeduplicationStatus(topicName)));
        Awaitility.await().untilAsserted(() -> assertFalse(messageDeduplication.isEnabled()));
        producer.newMessage().value("msg").sequenceId(1).send();
        checkDeduplicationDisabled(producerName, messageDeduplication);
        //remove namespace-level, use broker-level
        admin.namespaces().removeDeduplicationStatus(myNamespace);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getDeduplicationStatus(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication.isEnabled()));
        long maxSeq3 = sendMessageAndGetMaxSeq(maxMsgNum, producer);
        checkDeduplicationEnabled(producerName, messageDeduplication, maxSeq3);
    }

    @SuppressWarnings("unchecked")
    private long sendMessageAndGetMaxSeq(int maxMsgNum, Producer producer) throws Exception{
        long seq = System.nanoTime();
        for (int i = 0; i <= maxMsgNum; i++) {
            producer.newMessage().value("msg-" + i).sequenceId(seq + i).send();
        }
        return seq + maxMsgNum;
    }

    private void checkDeduplicationDisabled(String producerName, MessageDeduplication messageDeduplication)
            throws Exception {
        messageDeduplication.checkStatus().whenComplete((res, ex) -> {
            if (ex != null) {
                fail("should not fail");
            }
            assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), -1);
            assertEquals(messageDeduplication.highestSequencedPersisted.size(), 0);
            assertEquals(messageDeduplication.highestSequencedPushed.size(), 0);
        }).get();
    }

    private void checkDeduplicationEnabled(String producerName, MessageDeduplication messageDeduplication,
                                           long maxSeq) throws Exception {
        messageDeduplication.checkStatus().whenComplete((res, ex) -> {
            if (ex != null) {
                fail("should not fail");
            }
            assertNotNull(messageDeduplication.highestSequencedPersisted);
            assertNotNull(messageDeduplication.highestSequencedPushed);
            long seqId = messageDeduplication.getLastPublishedSequenceId(producerName);
            assertEquals(seqId, maxSeq);
            assertEquals(messageDeduplication.getHighestSequencedPersisted(producerName).longValue(), maxSeq);
            assertEquals(messageDeduplication.highestSequencedPushed.get(producerName).longValue(), maxSeq);
        }).get();
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 10000)
    public void testDuplicationSnapshotApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        waitCacheInit(topicName);
        Integer interval = admin.topics().getDeduplicationSnapshotInterval(topicName);
        assertNull(interval);

        admin.topics().setDeduplicationSnapshotInterval(topicName, 1024);
        Awaitility.await()
                .until(()-> admin.topics().getDeduplicationSnapshotInterval(topicName) != null);
        Assert.assertEquals(admin.topics().getDeduplicationSnapshotInterval(topicName).intValue(), 1024);

        admin.topics().removeDeduplicationSnapshotInterval(topicName);
        Awaitility.await()
                .until(()-> admin.topics().getDeduplicationSnapshotInterval(topicName) == null);
        assertNull(admin.topics().getDeduplicationSnapshotInterval(topicName));
    }

    @Test(timeOut = 30000)
    public void testTopicPolicyTakeSnapshot() throws Exception {
        cleanup();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(7);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        setup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false)
                .producerName(producerName).create();
        waitCacheInit(topicName);
        admin.topicPolicies().setDeduplicationSnapshotInterval(topicName, 3);
        admin.namespaces().setDeduplicationSnapshotInterval(myNamespace, 5);

        int msgNum = 10;
        CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value("msg" + i).sendAsync().whenComplete((res, e) -> countDownLatch.countDown());
        }
        countDownLatch.await();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        long seqId = persistentTopic.getMessageDeduplication().getHighestSequencedPersisted(producerName);
        Position position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertEquals(seqId, msgNum - 1);
        assertEquals(position.getEntryId(), msgNum - 1);
        //The first time, use topic-leve policies, 1 second delay + 3 second interval
        Awaitility.await()
                .until(() -> (persistentTopic.getMessageDeduplication().getManagedCursor()
                        .getMarkDeletedPosition()).getEntryId() == msgNum - 1);
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        Position markDeletedPosition = managedCursor.getMarkDeletedPosition();
        assertEquals(position, markDeletedPosition);

        //remove topic-level policies, namespace-level should be used, interval becomes 5 seconds
        admin.topicPolicies().removeDeduplicationSnapshotInterval(topicName);
        producer.newMessage().value("msg").send();
        //zk update time + 5 second interval time
        Awaitility.await()
                .until(() -> (persistentTopic.getMessageDeduplication().getManagedCursor()
                        .getMarkDeletedPosition()).getEntryId() == msgNum);
        markDeletedPosition = managedCursor.getMarkDeletedPosition();
        position = persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger()
                .getLastConfirmedEntry();
        assertEquals(msgNum, markDeletedPosition.getEntryId());
        assertEquals(position, markDeletedPosition);

        //4 remove namespace-level policies, broker-level should be used, interval becomes 3 seconds
        admin.namespaces().removeDeduplicationSnapshotInterval(myNamespace);
        Awaitility.await()
                .until(() -> (admin.namespaces().getDeduplicationSnapshotInterval(myNamespace) == null));
        producer.newMessage().value("msg").send();
        //ensure that the time exceeds the scheduling interval of ns and topic, but no snapshot is generated
        Thread.sleep(3000);
        markDeletedPosition = managedCursor.getMarkDeletedPosition();
        position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        // broker-level interval is 7 seconds, so 3 seconds will not take a snapshot
        assertNotEquals(msgNum + 1, markDeletedPosition.getEntryId());
        assertNotEquals(position, markDeletedPosition);
        // wait for scheduler
        Awaitility.await()
                .until(() -> (persistentTopic.getMessageDeduplication().getManagedCursor()
                        .getMarkDeletedPosition()).getEntryId() == msgNum + 1);
        markDeletedPosition = managedCursor.getMarkDeletedPosition();
        position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertEquals(msgNum + 1, markDeletedPosition.getEntryId());
        assertEquals(position, markDeletedPosition);
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 20000)
    public void testDuplicationMethod() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        final int maxMsgNum = 100;
        admin.topics().createPartitionedTopic(testTopic, 3);
        waitCacheInit(topicName);
        //1) Start up producer and send msg.We specified the max sequenceId
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .producerName(producerName).create();
        long maxSeq = sendMessageAndGetMaxSeq(maxMsgNum, producer);
        //2) Max sequenceId should be recorded correctly
        CompletableFuture<Optional<Topic>> completableFuture = pulsar.getBrokerService().getTopics().get(topicName);
        Topic topic = completableFuture.get(1, TimeUnit.SECONDS).get();
        PersistentTopic persistentTopic = (PersistentTopic) topic;
        MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
        checkDeduplicationEnabled(producerName, messageDeduplication, maxSeq);
        //3) disable the deduplication check
        admin.topics().enableDeduplication(topicName, false);
        Awaitility.await()
                .until(() -> admin.topics().getDeduplicationEnabled(topicName) != null);
        for (int i = 0; i < 100; i++) {
            producer.newMessage().value("msg-" + i).sequenceId(maxSeq + i).send();
        }
        //4) Max sequenceId record should be clear
        checkDeduplicationDisabled(producerName, messageDeduplication);

    }

    @Test(timeOut = 40000)
    public void testDuplicationSnapshot() throws Exception {
        testTakeSnapshot(true);
        testTakeSnapshot(false);
    }

    private void testTakeSnapshot(boolean enabledSnapshot) throws Exception {
        cleanup();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(enabledSnapshot ? 1 : 0);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        setup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false)
                .producerName(producerName).create();
        int msgNum = 50;
        CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value("msg" + i).sendAsync().whenComplete((res, e) -> countDownLatch.countDown());
        }
        countDownLatch.await();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicName).get().get();
        long seqId = persistentTopic.getMessageDeduplication().getHighestSequencedPersisted(producerName);
        Position position = persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger()
                .getLastConfirmedEntry();
        assertEquals(seqId, msgNum - 1);
        assertEquals(position.getEntryId(), msgNum - 1);

        Thread.sleep(2000);
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        Position markDeletedPosition = managedCursor.getMarkDeletedPosition();
        if (enabledSnapshot) {
            assertEquals(position, markDeletedPosition);
        } else {
            assertNotEquals(position, markDeletedPosition);
            assertNotEquals(markDeletedPosition.getEntryId(), -1);
        }

        producer.newMessage().value("msg").send();
        markDeletedPosition = managedCursor.getMarkDeletedPosition();
        position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertNotEquals(msgNum, markDeletedPosition.getEntryId());
        assertNotNull(position);

        Thread.sleep(2000);
        markDeletedPosition = managedCursor.getMarkDeletedPosition();
        position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        if (enabledSnapshot) {
            assertEquals(msgNum, markDeletedPosition.getEntryId());
            assertEquals(position, markDeletedPosition);
        } else {
            assertNotEquals(msgNum, markDeletedPosition.getEntryId());
            assertNotEquals(position, markDeletedPosition);
        }

    }

    @Test(timeOut = 30000)
    public void testNamespacePolicyApi() throws Exception {
        Integer interval = admin.namespaces().getDeduplicationSnapshotInterval(myNamespace);
        assertNull(interval);
        admin.namespaces().setDeduplicationSnapshotInterval(myNamespace, 100);
        interval = admin.namespaces().getDeduplicationSnapshotInterval(myNamespace);
        assertEquals(interval.intValue(), 100);
        admin.namespaces().removeDeduplicationSnapshotInterval(myNamespace);
        interval = admin.namespaces().getDeduplicationSnapshotInterval(myNamespace);
        assertNull(interval);

        admin.namespaces().setDeduplicationSnapshotIntervalAsync(myNamespace, 200).get();
        interval = admin.namespaces().getDeduplicationSnapshotIntervalAsync(myNamespace).get();
        assertEquals(interval.intValue(), 200);
        admin.namespaces().removeDeduplicationSnapshotIntervalAsync(myNamespace).get();
        interval = admin.namespaces().getDeduplicationSnapshotIntervalAsync(myNamespace).get();
        assertNull(interval);

    }

    @Test(timeOut = 30000)
    public void testNamespacePolicyTakeSnapshot() throws Exception {
        cleanup();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(3);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        setup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false)
                .producerName(producerName).create();
        admin.namespaces().setDeduplicationSnapshotInterval(myNamespace, 1);

        int msgNum = 50;
        CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value("msg" + i).sendAsync().whenComplete((res, e) -> countDownLatch.countDown());
        }
        countDownLatch.await();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicName).get().get();
        long seqId = persistentTopic.getMessageDeduplication().getHighestSequencedPersisted(producerName);
        Position position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertEquals(seqId, msgNum - 1);
        assertEquals(position.getEntryId(), msgNum - 1);
        //The first time, 1 second delay + 1 second interval
        Awaitility.await().until(()-> (persistentTopic
                .getMessageDeduplication().getManagedCursor().getMarkDeletedPosition()).getEntryId() == msgNum - 1);
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        Position markDeletedPosition = managedCursor.getMarkDeletedPosition();
        assertEquals(position, markDeletedPosition);
        //remove namespace-level policies, broker-level should be used
        admin.namespaces().removeDeduplicationSnapshotInterval(myNamespace);
        Thread.sleep(2000);
        markDeletedPosition = managedCursor.getMarkDeletedPosition();
        position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertNotEquals(msgNum - 1, markDeletedPosition.getEntryId());
        assertNotEquals(position, markDeletedPosition.getEntryId());
        //3 seconds total
        Thread.sleep(1000);
        markDeletedPosition = managedCursor.getMarkDeletedPosition();
        position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertEquals(msgNum - 1, markDeletedPosition.getEntryId());
        assertEquals(position, markDeletedPosition);

    }

    @Test(timeOut = 30000)
    public void testDisableNamespacePolicyTakeSnapshot() throws Exception {
        cleanup();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        setup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false).producerName(producerName).create();
        //set value to 0
        admin.namespaces().setDeduplicationSnapshotInterval(myNamespace, 0);

        int msgNum = 50;
        CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value("msg" + i).sendAsync().whenComplete((res, e) -> countDownLatch.countDown());
        }
        countDownLatch.await();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicName).get().get();
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        Position markDeletedPosition = managedCursor.getMarkDeletedPosition();

        long seqId = persistentTopic.getMessageDeduplication().getHighestSequencedPersisted(producerName);
        Position position = persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertEquals(seqId, msgNum - 1);
        assertEquals(position.getEntryId(), msgNum - 1);
        Awaitility.await().until(()-> (persistentTopic
                .getMessageDeduplication().getManagedCursor().getMarkDeletedPosition()).getEntryId() == -1);

        // take snapshot is disabled, so markDeletedPosition should not change
        assertEquals(markDeletedPosition, managedCursor.getMarkDeletedPosition());
        assertEquals(markDeletedPosition.getEntryId(), -1);
        assertNotEquals(position, markDeletedPosition);

    }

    @Test(timeOut = 30000)
    public void testDisableNamespacePolicyTakeSnapshotShouldNotThrowException() throws Exception {
        cleanup();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        setup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false).producerName(producerName).create();

        // disable deduplication
        admin.namespaces().setDeduplicationStatus(myNamespace, false);

        int msgNum = 50;
        CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value("msg" + i).sendAsync().whenComplete((res, e) -> countDownLatch.countDown());
        }
        countDownLatch.await();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicName).get().get();
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();

        // when disable topic deduplication the cursor should be deleted.
        assertNull(managedCursor);

        // this method will be called at brokerService forEachTopic.
        // if topic level disable deduplication.
        // this method should be skipped without throw exception.
        persistentTopic.checkDeduplicationSnapshot();
    }

    @Test
    public void testFinishTakeSnapshotWhenTopicLoading() throws Exception {
        // Create a topic and wait deduplication is started.
        int brokerDeduplicationEntriesInterval = 1000;
        pulsar.getConfiguration().setBrokerDeduplicationEnabled(true);
        pulsar.getConfiguration().setBrokerDeduplicationEntriesInterval(brokerDeduplicationEntriesInterval);
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topic);
        final PersistentTopic persistentTopic1 =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        final ManagedLedgerImpl ml1 = (ManagedLedgerImpl) persistentTopic1.getManagedLedger();
        Awaitility.await().untilAsserted(() -> {
            ManagedCursorImpl cursor1 =
                    (ManagedCursorImpl) ml1.getCursors().get(PersistentTopic.DEDUPLICATION_CURSOR_NAME);
            assertNotNull(cursor1);
        });
        final MessageDeduplication deduplication1 = persistentTopic1.getMessageDeduplication();


        // Send 999 messages, it is less than "brokerDeduplicationEntriesInterval".
        // So it would not trigger takeSnapshot
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).enableBatching(false).create();
        for (int i = 0; i < brokerDeduplicationEntriesInterval - 1; i++) {
            producer.send(i + "");
        }
        producer.close();
        int snapshotCounter1 = WhiteboxImpl.getInternalState(deduplication1, "snapshotCounter");
        assertEquals(snapshotCounter1, brokerDeduplicationEntriesInterval - 1);


        // Unload and load topic, simulate topic load is timeout.
        // SetBrokerDeduplicationEntriesInterval to 10, therefore recoverSequenceIdsMap#takeSnapshot
        // would trigger and should update the snapshot position.
        // However, if topic close and takeSnapshot are concurrent,
        // it would result in takeSnapshot throw exception
        admin.topics().unload(topic);
        pulsar.getConfiguration().setBrokerDeduplicationEntriesInterval(10);

        // Mock message deduplication recovery speed topicLoadTimeoutSeconds
        pulsar.getConfiguration().setTopicLoadTimeoutSeconds(1);
        String mlPath = BrokerService.MANAGED_LEDGER_PATH_ZNODE + "/"
                + TopicName.get(topic).getPersistenceNamingEncoding() + "/" + DEDUPLICATION_CURSOR_NAME;
        mockZooKeeper.delay(2 * 1000, (op, path) -> {
            if (mlPath.equals(path)) {
                return true;
            }
            return false;
        });

        final var topics = pulsar.getBrokerService().getTopics();
        try {
            pulsar.getBrokerService().getTopic(topic, false).join().get();
            Assert.fail();
        } catch (Exception e) {
            // topic loading should timeout.
        }
        Awaitility.await().untilAsserted(() -> {
            // topic loading timeout then close topic and remove from topicsMap
            Assert.assertFalse(topics.containsKey(topic));
        });


        // Load topic again, setBrokerDeduplicationEntriesInterval to 10000,
        // make recoverSequenceIdsMap#takeSnapshot not trigger takeSnapshot.
        // But actually it should not replay again in recoverSequenceIdsMap,
        // since previous topic loading should finish the replay process.
        pulsar.getConfiguration().setBrokerDeduplicationEntriesInterval(10000);
        pulsar.getConfiguration().setTopicLoadTimeoutSeconds(60);
        PersistentTopic persistentTopic2 =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        ManagedLedgerImpl ml2 = (ManagedLedgerImpl) persistentTopic2.getManagedLedger();
        MessageDeduplication deduplication2 = persistentTopic2.getMessageDeduplication();

        Awaitility.await().untilAsserted(() -> {
            int snapshotCounter3 = WhiteboxImpl.getInternalState(deduplication2, "snapshotCounter");
            Assert.assertEquals(snapshotCounter3, 0);
            Assert.assertEquals(ml2.getLedgersInfo().size(), 1);
        });


        // cleanup.
        admin.topics().delete(topic);
    }

    @Test(timeOut = 120000, dataProvider = "deduplicationBatching")
    public void testDeduplicationContractAfterLostReceiptsAndRestart(boolean batching) throws Exception {
        String namespace = testTenant + "/dedup-recovery-" + UUID.randomUUID();
        String topic = "persistent://" + namespace + "/telemetry";
        String subscription = "dedup-contract";
        int messagesPerProducer = 12;
        int producerCount = 2;
        int initialMessages = messagesPerProducer * producerCount;
        Map<ProducerSequence, MessageId> storedIds = new HashMap<>();
        AtomicBoolean discardReceipts = new AtomicBoolean(true);
        CountDownLatch persistedReceipts = new CountDownLatch(batching ? producerCount : initialMessages);
        admin.namespaces().createNamespace(namespace);
        try {
            admin.namespaces().setDeduplicationStatus(namespace, true);
            admin.namespaces().setDeduplicationSnapshotInterval(namespace, 0);
            ClientBuilderImpl builder = (ClientBuilderImpl) PulsarClient.builder()
                    .serviceUrl(lookupUrl.toString());
            try (PulsarClient faultClient = InjectedClientCnxClientBuilder.create(builder,
                    (clientConfig, eventLoopGroup) -> new ClientCnx(
                            InstrumentProvider.NOOP, clientConfig, eventLoopGroup) {
                        @Override
                        protected void handleSendReceipt(CommandSendReceipt receipt) {
                            if (discardReceipts.get()) {
                                // The broker persisted the entry, but the producer cannot know that it succeeded.
                                persistedReceipts.countDown();
                            } else {
                                super.handleSendReceipt(receipt);
                            }
                        }
                    })) {
                List<CompletableFuture<MessageId>> pending = new ArrayList<>();
                // The client owns these producers, including cleanup if an assertion fails while receipts are lost.
                List<Producer<String>> producers = new ArrayList<>();
                try (Consumer<String> consumer = faultClient.newConsumer(Schema.STRING).topic(topic)
                        .subscriptionName(subscription).subscribe()) {
                    for (int p = 0; p < producerCount; p++) {
                        Producer<String> producer = faultClient.newProducer(Schema.STRING).topic(topic)
                                .producerName("gateway-" + p).enableBatching(batching)
                                .batchingMaxMessages(messagesPerProducer)
                                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                                .maxPendingMessages(initialMessages).sendTimeout(0, TimeUnit.SECONDS).create();
                        producers.add(producer);
                        for (int sequence = 0; sequence < messagesPerProducer; sequence++) {
                            pending.add(producer.newMessage().sequenceId(sequence)
                                    .value(producer.getProducerName() + ":" + sequence).sendAsync());
                        }
                    }
                    assertThat(persistedReceipts.await(15, TimeUnit.SECONDS))
                            .as("the lost-receipt fault must affect persisted publishes").isTrue();
                    assertThat(pending).allMatch(future -> !future.isDone());
                    while (storedIds.size() < initialMessages) {
                        recordStoredMessage(storedIds, consumer.receive(10, TimeUnit.SECONDS));
                    }
                    assertThat(storedIds).hasSize(initialMessages);
                    // Close without acknowledgements: the recovered subscription must legitimately redeliver these.
                }

                // This restarts the broker service over the same BookKeeper/metadata fixtures, not a process crash.
                restartBroker(ignored -> discardReceipts.set(false));
                faultClient.updateServiceUrl(lookupUrl.toString());
                for (CompletableFuture<MessageId> publish : pending) {
                    publish.get(30, TimeUnit.SECONDS);
                }
                for (Producer<String> producer : producers) {
                    // Retry inside the recovered batch before newer writes can raise the high-water mark.
                    // The automatic resend starts at 0, which even an incorrectly recovered high-water mark rejects.
                    for (int sequence = 1; sequence < messagesPerProducer; sequence++) {
                        pending.add(producer.newMessage().sequenceId(sequence)
                                .value(producer.getProducerName() + ":" + sequence).sendAsync());
                    }
                    producer.flushAsync().get(15, TimeUnit.SECONDS);
                    for (int sequence = messagesPerProducer; sequence < messagesPerProducer * 2; sequence++) {
                        pending.add(producer.newMessage().sequenceId(sequence)
                                .value(producer.getProducerName() + ":" + sequence).sendAsync());
                    }
                    producer.flushAsync().get(15, TimeUnit.SECONDS);
                    // Retry OLD sequences only after newer writes succeed. A latest-message-only oracle misses these.
                    for (int sequence = 0; sequence < messagesPerProducer; sequence++) {
                        pending.add(producer.newMessage().sequenceId(sequence)
                                .value(producer.getProducerName() + ":" + sequence).sendAsync());
                    }
                    producer.flushAsync().get(15, TimeUnit.SECONDS);
                }
                for (CompletableFuture<MessageId> publish : pending) {
                    publish.get(15, TimeUnit.SECONDS);
                }
                for (Producer<String> producer : producers) {
                    producer.close();
                }

                try (Consumer<String> consumer = faultClient.newConsumer(Schema.STRING).topic(topic)
                        .subscriptionName(subscription).subscribe()) {
                    Map<ProducerSequence, MessageId> recoveredIds = new HashMap<>();
                    while (recoveredIds.size() < initialMessages * 2) {
                        Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                        recordStoredMessage(storedIds, message);
                        // Count unique sequences, not deliveries: any number of same-ID redeliveries is legitimate.
                        recordStoredMessage(recoveredIds, message);
                    }
                    assertThat(storedIds).hasSize(initialMessages * 2);
                    assertThat(recoveredIds).as("all unacknowledged messages must redeliver with their original IDs")
                            .containsAllEntriesOf(storedIds);
                }

                // Scan the entire retained backlog, not just until every unique sequence has been seen. Otherwise
                // persisted duplicates at the tail can be hidden by an early completeness-based success.
                try (Reader<String> reader = faultClient.newReader(Schema.STRING).topic(topic)
                        .startMessageId(MessageId.earliest).create()) {
                    Map<ProducerSequence, MessageId> scannedIds = new HashMap<>();
                    while (reader.hasMessageAvailableAsync().get(10, TimeUnit.SECONDS)) {
                        Message<String> message = reader.readNextAsync().get(10, TimeUnit.SECONDS);
                        recordStoredMessage(storedIds, message);
                        recordStoredMessage(scannedIds, message);
                        assertThat(scannedIds.size()).isLessThanOrEqualTo(initialMessages * 2);
                    }
                    assertThat(scannedIds).hasSize(initialMessages * 2).containsAllEntriesOf(storedIds);
                }
            }
        } finally {
            for (String createdTopic : admin.topics().getList(namespace)) {
                admin.topics().delete(createdTopic, true);
            }
            admin.namespaces().deleteNamespace(namespace);
        }
    }

    @DataProvider
    public Object[][] deduplicationBatching() {
        return new Object[][] {{false}, {true}};
    }

    @Test
    public void testStoredSequenceOracleAllowsRedeliveryButRejectsRepublishing() {
        Map<ProducerSequence, MessageId> storedIds = new HashMap<>();
        MessageId first = new BatchMessageIdImpl(1, 2, -1, 0);
        recordStoredId(storedIds, "gateway-a", 0, first);
        recordStoredId(storedIds, "gateway-a", 1, new MessageIdImpl(1, 3, -1));
        recordStoredId(storedIds, "gateway-b", 0, new MessageIdImpl(1, 4, -1));
        recordStoredId(storedIds, "gateway-a", 0, new BatchMessageIdImpl(1, 2, -1, 0));
        assertThat(storedIds).hasSize(3);
        // Even an old sequence at a different batch index in the SAME entry is a distinct persisted copy.
        assertThatThrownBy(() -> recordStoredId(storedIds, "gateway-a", 0,
                new BatchMessageIdImpl(1, 2, -1, 1))).isInstanceOf(AssertionError.class)
                .hasMessageContaining("persisted more than once");
        assertThatThrownBy(() -> recordStoredId(storedIds, "gateway-a", 0,
                new MessageIdImpl(2, 0, -1))).isInstanceOf(AssertionError.class)
                .hasMessageContaining("persisted more than once");
    }

    private record ProducerSequence(String producer, long sequence) { }

    private static void recordStoredMessage(Map<ProducerSequence, MessageId> storedIds, Message<String> message) {
        assertThat(message).as("expected a stored message").isNotNull();
        assertThat(message.getValue()).isEqualTo(message.getProducerName() + ":" + message.getSequenceId());
        recordStoredId(storedIds, message.getProducerName(), message.getSequenceId(), message.getMessageId());
    }

    private static void recordStoredId(Map<ProducerSequence, MessageId> storedIds, String producer, long sequence,
                                       MessageId messageId) {
        ProducerSequence key = new ProducerSequence(producer, sequence);
        MessageId original = storedIds.putIfAbsent(key, messageId);
        if (original != null) {
            assertThat(messageId).as("%s persisted more than once; original stored ID: %s", key, original)
                    .isEqualTo(original);
        }
    }

    private void waitCacheInit(String topicName) throws Exception {
        pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe().close();
        TopicName topic = TopicName.get(topicName);
    }

}
