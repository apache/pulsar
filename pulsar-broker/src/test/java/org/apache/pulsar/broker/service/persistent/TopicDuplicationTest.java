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
package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicDuplicationTest extends ProducerConsumerBase {
    private final String testTenant = "my-property";
    private final String testNamespace = "my-ns";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/max-unacked-";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        resetConfig();
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        this.conf.setBrokerDeduplicationEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

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
        Awaitility.await().untilAsserted(() -> assertEquals(admin.topics().getDeduplicationStatus(topicName, true).booleanValue(),
                conf.isBrokerDeduplicationEnabled()));
    }

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
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getDeduplicationStatus(myNamespace)));
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

    private long sendMessageAndGetMaxSeq(int maxMsgNum, Producer producer) throws Exception{
        long seq = System.nanoTime();
        for (int i = 0; i <= maxMsgNum; i++) {
            producer.newMessage().value("msg-" + i).sequenceId(seq + i).send();
        }
        return seq + maxMsgNum;
    }

    private void checkDeduplicationDisabled(String producerName, MessageDeduplication messageDeduplication) throws Exception {
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
            assertEquals(messageDeduplication.highestSequencedPersisted.get(producerName).longValue(), maxSeq);
            assertEquals(messageDeduplication.highestSequencedPushed.get(producerName).longValue(), maxSeq);
        }).get();
    }

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
        super.internalCleanup();
        resetConfig();
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(7);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        super.internalSetup();
        super.producerBaseSetup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false).producerName(producerName).create();
        waitCacheInit(topicName);
        admin.topicPolicies().setDeduplicationSnapshotInterval(topicName, 3);
        admin.namespaces().setDeduplicationSnapshotInterval(myNamespace, 5);

        int msgNum = 10;
        CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value("msg" + i).sendAsync().whenComplete((res, e) -> countDownLatch.countDown());
        }
        countDownLatch.await();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        long seqId = persistentTopic.getMessageDeduplication().highestSequencedPersisted.get(producerName);
        PositionImpl position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertEquals(seqId, msgNum - 1);
        assertEquals(position.getEntryId(), msgNum - 1);
        //The first time, use topic-leve policies, 1 second delay + 3 second interval
        Awaitility.await()
                .until(() -> ((PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor()
                        .getMarkDeletedPosition()).getEntryId() == msgNum - 1);
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        PositionImpl markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        assertEquals(position, markDeletedPosition);

        //remove topic-level policies, namespace-level should be used, interval becomes 5 seconds
        admin.topicPolicies().removeDeduplicationSnapshotInterval(topicName);
        producer.newMessage().value("msg").send();
        //zk update time + 5 second interval time
        Awaitility.await()
                .until(() -> ((PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor()
                        .getMarkDeletedPosition()).getEntryId() == msgNum);
        markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
        assertEquals(msgNum, markDeletedPosition.getEntryId());
        assertEquals(position, markDeletedPosition);

        //4 remove namespace-level policies, broker-level should be used, interval becomes 3 seconds
        admin.namespaces().removeDeduplicationSnapshotInterval(myNamespace);
        Awaitility.await()
                .until(() -> (admin.namespaces().getDeduplicationSnapshotInterval(myNamespace) == null));
        producer.newMessage().value("msg").send();
        //ensure that the time exceeds the scheduling interval of ns and topic, but no snapshot is generated
        Thread.sleep(3000);
        markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
        // broker-level interval is 7 seconds, so 3 seconds will not take a snapshot
        assertNotEquals(msgNum + 1, markDeletedPosition.getEntryId());
        assertNotEquals(position, markDeletedPosition);
        // wait for scheduler
        Awaitility.await()
                .until(() -> ((PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor()
                        .getMarkDeletedPosition()).getEntryId() == msgNum + 1);
        markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
        assertEquals(msgNum + 1, markDeletedPosition.getEntryId());
        assertEquals(position, markDeletedPosition);
    }

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
        super.internalCleanup();
        resetConfig();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(enabledSnapshot ? 1 : 0);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        super.internalSetup();
        super.producerBaseSetup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false).producerName(producerName).create();
        int msgNum = 50;
        CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value("msg" + i).sendAsync().whenComplete((res, e) -> countDownLatch.countDown());
        }
        countDownLatch.await();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        long seqId = persistentTopic.getMessageDeduplication().highestSequencedPersisted.get(producerName);
        PositionImpl position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
        assertEquals(seqId, msgNum - 1);
        assertEquals(position.getEntryId(), msgNum - 1);

        Thread.sleep(2000);
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        PositionImpl markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        if (enabledSnapshot) {
            assertEquals(position, markDeletedPosition);
        } else {
            assertNotEquals(position, markDeletedPosition);
            assertNotEquals(markDeletedPosition.getEntryId(), -1);
        }

        producer.newMessage().value("msg").send();
        markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
        assertNotEquals(msgNum, markDeletedPosition.getEntryId());
        assertNotNull(position);

        Thread.sleep(2000);
        markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
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
        super.internalCleanup();
        resetConfig();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(3);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        super.internalSetup();
        super.producerBaseSetup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false).producerName(producerName).create();
        admin.namespaces().setDeduplicationSnapshotInterval(myNamespace, 1);

        int msgNum = 50;
        CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value("msg" + i).sendAsync().whenComplete((res, e) -> countDownLatch.countDown());
        }
        countDownLatch.await();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        long seqId = persistentTopic.getMessageDeduplication().highestSequencedPersisted.get(producerName);
        PositionImpl position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertEquals(seqId, msgNum - 1);
        assertEquals(position.getEntryId(), msgNum - 1);
        //The first time, 1 second delay + 1 second interval
        Awaitility.await().until(()-> ((PositionImpl) persistentTopic
                .getMessageDeduplication().getManagedCursor().getMarkDeletedPosition()).getEntryId() == msgNum -1);
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        PositionImpl markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        assertEquals(position, markDeletedPosition);
        //remove namespace-level policies, broker-level should be used
        admin.namespaces().removeDeduplicationSnapshotInterval(myNamespace);
        Thread.sleep(2000);
        markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
        assertNotEquals(msgNum - 1, markDeletedPosition.getEntryId());
        assertNotEquals(position, markDeletedPosition.getEntryId());
        //3 seconds total
        Thread.sleep(1000);
        markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
        assertEquals(msgNum - 1, markDeletedPosition.getEntryId());
        assertEquals(position, markDeletedPosition);

    }

    @Test(timeOut = 30000)
    public void testDisableNamespacePolicyTakeSnapshot() throws Exception {
        super.internalCleanup();
        resetConfig();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        super.internalSetup();
        super.producerBaseSetup();

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
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        PositionImpl markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();

        long seqId = persistentTopic.getMessageDeduplication().highestSequencedPersisted.get(producerName);
        PositionImpl position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor()
                .getManagedLedger().getLastConfirmedEntry();
        assertEquals(seqId, msgNum - 1);
        assertEquals(position.getEntryId(), msgNum - 1);
        Awaitility.await().until(()-> ((PositionImpl) persistentTopic
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

    private void waitCacheInit(String topicName) throws Exception {
        pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe().close();
        TopicName topic = TopicName.get(topicName);
    }

}
