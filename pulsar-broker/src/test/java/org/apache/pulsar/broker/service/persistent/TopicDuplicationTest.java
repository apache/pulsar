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
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

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
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(()-> admin.topics().getDeduplicationEnabled(topicName) != null);
        Assert.assertEquals(admin.topics().getDeduplicationEnabled(topicName), true);

        admin.topics().disableDeduplication(topicName);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(()-> admin.topics().getMaxUnackedMessagesOnSubscription(topicName) == null);
        assertNull(admin.topics().getDeduplicationEnabled(topicName));
    }

    @Test(timeOut = 10000)
    public void testDuplicationSnapshotApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        waitCacheInit(topicName);
        Integer interval = admin.topics().getDeduplicationSnapshotInterval(topicName);
        assertNull(interval);

        admin.topics().setDeduplicationSnapshotInterval(topicName, 1024);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(()-> admin.topics().getDeduplicationSnapshotInterval(topicName) != null);
        Assert.assertEquals(admin.topics().getDeduplicationSnapshotInterval(topicName).intValue(), 1024);

        admin.topics().removeDeduplicationSnapshotInterval(topicName);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(()-> admin.topics().getDeduplicationSnapshotInterval(topicName) == null);
        assertNull(admin.topics().getDeduplicationSnapshotInterval(topicName));
    }

    @Test(timeOut = 30000)
    public void testTopicPolicyTakeSnapshot() throws Exception {
        resetConfig();
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(7);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        super.internalCleanup();
        super.internalSetup();
        super.producerBaseSetup();

        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING).topic(topicName).enableBatching(false).producerName(producerName).create();
        waitCacheInit(topicName);
        admin.topics().setDeduplicationSnapshotInterval(topicName, 3);
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
        Awaitility.await().atMost(5000, TimeUnit.MILLISECONDS)
                .until(() -> ((PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor()
                        .getMarkDeletedPosition()).getEntryId() == msgNum - 1);
        ManagedCursor managedCursor = persistentTopic.getMessageDeduplication().getManagedCursor();
        PositionImpl markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        assertEquals(position, markDeletedPosition);

        //remove topic-level policies, namespace-level should be used, interval becomes 5 seconds
        admin.topics().removeDeduplicationSnapshotInterval(topicName);
        producer.newMessage().value("msg").send();
        //zk update time + 5 second interval time
        Awaitility.await().atMost( 7, TimeUnit.SECONDS)
                .until(() -> ((PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor()
                        .getMarkDeletedPosition()).getEntryId() == msgNum);
        markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        position = (PositionImpl) persistentTopic.getMessageDeduplication().getManagedCursor().getManagedLedger().getLastConfirmedEntry();
        assertEquals(msgNum, markDeletedPosition.getEntryId());
        assertEquals(position, markDeletedPosition);

        //4 remove namespace-level policies, broker-level should be used, interval becomes 3 seconds
        admin.namespaces().removeDeduplicationSnapshotInterval(myNamespace);
        Awaitility.await().atMost(4, TimeUnit.SECONDS)
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
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
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
        long seq = System.currentTimeMillis();
        for (int i = 0; i <= maxMsgNum; i++) {
            producer.newMessage().value("msg-" + i).sequenceId(seq + i).send();
        }
        long maxSeq = seq + maxMsgNum;
        //2) Max sequenceId should be recorded correctly
        CompletableFuture<Optional<Topic>> completableFuture = pulsar.getBrokerService().getTopics().get(topicName);
        Topic topic = completableFuture.get(1, TimeUnit.SECONDS).get();
        PersistentTopic persistentTopic = (PersistentTopic) topic;
        MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
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
        //3) disable the deduplication check
        admin.topics().enableDeduplication(topicName, false);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> admin.topics().getDeduplicationEnabled(topicName) != null);
        for (int i = 0; i < 100; i++) {
            producer.newMessage().value("msg-" + i).sequenceId(maxSeq + i).send();
        }
        //4) Max sequenceId record should be clear
        messageDeduplication.checkStatus().whenComplete((res, ex) -> {
            if (ex != null) {
                fail("should not fail");
            }
            assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), -1);
            assertEquals(messageDeduplication.highestSequencedPersisted.size(), 0);
            assertEquals(messageDeduplication.highestSequencedPushed.size(), 0);
        }).get();

    }

    @Test(timeOut = 40000)
    public void testDuplicationSnapshot() throws Exception {
        testTakeSnapshot(true);
        testTakeSnapshot(false);
    }

    private void testTakeSnapshot(boolean enabledSnapshot) throws Exception {
        resetConfig();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(enabledSnapshot ? 1 : 0);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        super.internalCleanup();
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
    private void testNamespacePolicyTakeSnapshot() throws Exception {
        resetConfig();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(3);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        super.internalCleanup();
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
        Awaitility.await().atMost(2500,TimeUnit.MILLISECONDS).until(()-> ((PositionImpl) persistentTopic
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
    private void testDisableNamespacePolicyTakeSnapshot() throws Exception {
        resetConfig();
        conf.setBrokerDeduplicationEnabled(true);
        conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        conf.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        conf.setBrokerDeduplicationEntriesInterval(20000);
        super.internalCleanup();
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
        Awaitility.await().atMost(2,TimeUnit.SECONDS).until(()-> ((PositionImpl) persistentTopic
                .getMessageDeduplication().getManagedCursor().getMarkDeletedPosition()).getEntryId() == -1);

        // take snapshot is disabled, so markDeletedPosition should not change
        assertEquals(markDeletedPosition, managedCursor.getMarkDeletedPosition());
        assertEquals(markDeletedPosition.getEntryId(), -1);
        assertNotEquals(position, markDeletedPosition);

    }

    private void waitCacheInit(String topicName) throws Exception {
        pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe().close();
        TopicName topic = TopicName.get(topicName);
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(()-> pulsar.getTopicPoliciesService().cacheIsInitialized(topic));
    }

}
