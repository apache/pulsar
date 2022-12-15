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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.shaded.org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class BatchedMessageDispatchThrottlingTest extends ProducerConsumerBase {

    private static final int MIN_ENTRY_COUNT_PER_DISPATCH = 5;

    private static final int MAX_ENTRY_COUNT_PER_DISPATCH = 120;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setClusterName("test");
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setPreciseDispatcherFlowControl(true);
        conf.setDispatcherMaxReadBatchSize(MAX_ENTRY_COUNT_PER_DISPATCH);
        conf.setDispatcherMinReadBatchSize(MIN_ENTRY_COUNT_PER_DISPATCH);
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);
    }

    private void triggerTopicAndSubCreates(String topicName, String subName, SubscriptionType subscriptionType)
            throws Exception{
        Consumer consumer = pulsarClient.newConsumer(Schema.JSON(String.class))
                .topic(topicName)
                .readCompacted(false)
                .receiverQueueSize(0)
                .subscriptionName(subName)
                .subscriptionType(subscriptionType)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        consumer.close();
    }

    private List<Consumer<String>> createConsumes(String topicName, String subName, int consumerCount,
                                               SubscriptionType subscriptionType, int queueSize) throws Exception{
        List<Consumer<String>> consumers = new ArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            consumers.add(pulsarClient.newConsumer(Schema.JSON(String.class))
                    .topic(topicName)
                    .ackTimeout(Integer.MAX_VALUE, TimeUnit.SECONDS)
                    .ackTimeoutTickTime(Integer.MAX_VALUE, TimeUnit.SECONDS)
                    .readCompacted(false)
                    .receiverQueueSize(queueSize)
                    .subscriptionName(subName)
                    .subscriptionType(subscriptionType)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscribe());
        }
        return consumers;
    }

    private void sendManyBatchedMessages(int msgCountPerEntry, int entryCount, String topicName)
            throws Exception {
        sendManyBatchedMessages(msgCountPerEntry, entryCount, topicName, "1");
    }

    private void sendManyBatchedMessages(int msgCountPerEntry, int entryCount, String topicName, String key)
            throws Exception {
        Producer<String> producer = pulsarClient.newProducer(Schema.JSON(String.class))
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(Integer.MAX_VALUE, TimeUnit.SECONDS)
                .batchingMaxMessages(Integer.MAX_VALUE)
                .create();
        for (int i = 0; i < entryCount; i++){
            for (int j = 0; j < msgCountPerEntry; j++){
                producer.newMessage().key(key).value(String.format("entry-seq[%s], batch_index[%s]", i, j)).sendAsync();
            }
            producer.flush();
        }
        producer.close();
    }

    private ConsumerKnownHowManyMessagesPerBatch letOneConsumerKnowAvgMsgPerEntryAndReceiveOneEntry(
            String topicName, String subName, SubscriptionType subType) throws Exception {
        // To ensure just receive one entry, update rate limit to 1msg/s.
        DispatchRate originalDispatchRate = admin.topicPolicies().getSubscriptionDispatchRate(topicName);
        setSubscriptionDispatchRate(topicName, 1, -1, originalDispatchRate.getRatePeriodInSecond());
        // Receive one entry and rewind.
        Consumer<String> consumer = createConsumes(topicName, subName, 1, subType, 1).get(0);
        waitForReceivedEntryCount(consumer, Duration.ofSeconds(3), entryCount -> entryCount == 1);
        Set<Long> receivedEntryIds = collectIncomingQueueEntryIds(consumer);
        // Reset dispatch rate.
        setSubscriptionDispatchRate(topicName, originalDispatchRate.getDispatchThrottlingRateInMsg(),
                originalDispatchRate.getDispatchThrottlingRateInByte(), originalDispatchRate.getRatePeriodInSecond());
        return new ConsumerKnownHowManyMessagesPerBatch(consumer, receivedEntryIds);
    }

    @AllArgsConstructor
    private static class ConsumerKnownHowManyMessagesPerBatch {
        private Consumer<String> consumer;
        private Set<Long> receivedEntries;
    }

    private void triggerManagedLedgerStatUpdate() throws Exception {
        ManagedLedgerFactoryImpl managedLedgerFactory =
                (ManagedLedgerFactoryImpl) pulsar.getBrokerService().getManagedLedgerFactory();
        Method method = ManagedLedgerFactoryImpl.class.getDeclaredMethod("refreshStats", new Class[]{});
        method.setAccessible(true);
        method.invoke(managedLedgerFactory);
    }

    private LinkedHashSet<Long> collectIncomingQueueEntryIds(Consumer<String> consumer) {
        return collectIncomingQueueEntryIds(Arrays.asList(consumer));
    }

    private LinkedHashSet<Long> collectIncomingQueueEntryIds(List<Consumer<String>> consumers) {
        List<BlockingQueue<Message<String>>> allIncomingQueue = new ArrayList<>();
        for (Consumer consumer : consumers){
            if (consumer instanceof MultiTopicsConsumerImpl multiTopicsConsumer){
                List<ConsumerImpl<String>> consumerImplList = multiTopicsConsumer.getConsumers();
                for (Consumer<String> consumerImpl : consumerImplList){
                    BlockingQueue<Message<String>> incomingMessages =
                            WhiteboxImpl.getInternalState(consumerImpl, "incomingMessages");
                    allIncomingQueue.add(incomingMessages);
                }
            } else if (consumer instanceof ConsumerImpl consumerImpl){
                BlockingQueue<Message<String>> incomingMessages =
                        WhiteboxImpl.getInternalState(consumerImpl, "incomingMessages");
                allIncomingQueue.add(incomingMessages);
            }
        }

        LinkedHashSet<Long> entryIds = new LinkedHashSet<>();
        for (BlockingQueue<Message<String>> incomingQueue : allIncomingQueue){
            ArrayList<Message<String>> messages = new ArrayList<>();
            while (incomingQueue.peek() != null){
                Message<String> message = incomingQueue.poll();
                messages.add(message);
                entryIds.add(((MessageIdImpl)message.getMessageId()).getEntryId());
            }
            for (Message<String> message : messages){
                incomingQueue.add(message);
            }
        }
        return entryIds;
    }

    private void assertWillNotReceiveMessagesAnyMore(final List<Consumer<String>> consumers, Duration waitTime,
                                                     int alreadyReceivedEntryCount){
        try {
            waitForAnyConsumerHasReceivedAnyNewMsg(consumers, waitTime, alreadyReceivedEntryCount);
            fail("Expected no messages received");
        } catch (Exception ex){
            // Expected no messages received.
        }
    }

    private void waitForAnyConsumerHasReceivedAnyNewMsg(final List<Consumer<String>> consumers, Duration maxWaitTime,
                                                        int alreadyReceivedMsgCount) {
        Awaitility.await().atMost(maxWaitTime).until(() -> {
            int currentReceivedEntryCount = collectIncomingQueueEntryIds(consumers).size();
            return currentReceivedEntryCount > alreadyReceivedMsgCount;
        });
    }

    private void waitForReceivedEntryCount(final Consumer<String> consumer, Duration maxWaitTime,
                                           Predicate<Integer> entryCountPredicate) {
        waitForReceivedEntryCount(Arrays.asList(consumer), maxWaitTime, entryCountPredicate);
    }

    private void waitForReceivedEntryCount(final List<Consumer<String>> consumers, Duration maxWaitTime,
                                           Predicate<Integer> entryCountPredicate) {
        Awaitility.await().atMost(maxWaitTime).until(() -> {
            return entryCountPredicate.test(collectIncomingQueueEntryIds(consumers).size());
        });
    }

    private void closeConsumers(Consumer...consumer) throws Exception {
        closeConsumers(Arrays.asList(consumer));
    }

    private void closeConsumers(Collection<? extends Consumer> consumers) throws Exception {
        for (Consumer<String> consumer : consumers){
            consumer.close();
        }
    }

    private void rewind(Collection<? extends Consumer> consumers) throws Exception {
        if (consumers.isEmpty()){
            throw new IllegalArgumentException("consumers collection is empty");
        }
        consumers.iterator().next().seek(MessageIdImpl.earliest);
    }

    private void setSubscriptionDispatchRate(String topicName, int msgCount, long bytesSize, int ratePeriodInSecond)
            throws PulsarAdminException {
        DispatchRateImpl dispatchRate = new DispatchRateImpl();
        dispatchRate.setDispatchThrottlingRateInMsg(msgCount);
        dispatchRate.setDispatchThrottlingRateInByte(bytesSize);
        dispatchRate.setRatePeriodInSecond(ratePeriodInSecond);
        admin.topicPolicies().setSubscriptionDispatchRate(topicName, dispatchRate);
        waitDispatchRateLimitSetFinish(topicName, msgCount, bytesSize);
    }

    private void waitDispatchRateLimitSetFinish(String topicName, long msgCount, long bytesSize) {
        Awaitility.await().untilAsserted(() -> {
            List<? extends Subscription> subscriptions = pulsar.getBrokerService()
                    .getTopic(topicName, false).get().get().getSubscriptions().values();
            for (Subscription subscription : subscriptions){
                PersistentSubscription persistentSubscription = (PersistentSubscription) subscription;
                Optional<DispatchRateLimiter> rateLimiterOptional =
                        persistentSubscription.getDispatcher().getRateLimiter();
                if (!rateLimiterOptional.isPresent()){
                    fail("rate limiter is null.");
                }
                DispatchRateLimiter rateLimiter = rateLimiterOptional.get();
                assertEquals(rateLimiter.getDispatchRateOnMsg(), msgCount);
                assertEquals(rateLimiter.getDispatchRateOnByte(), bytesSize);
            }
        });

    }

    @DataProvider(name = "subTypes")
    public Object[][] subTypes(){
        return new Object[][]{
                {SubscriptionType.Shared},
                {SubscriptionType.Key_Shared},
                {SubscriptionType.Failover}
        };
    }

    /**
     * A wrong logic verify: Consumers will receive overdose messages. this is a bug: when enabled batch rate limit no
     * precise even if set {conf.preciseDispatcherFlowControl} to `true`. TODO will be fixed by PR: #18581.
     * Because the broker could not know how many messages per entry then broker think that an entry as one message.
     * so will receive {rateLimitMsgCountPerPeriod} entries.
     */
    @Test(dataProvider = "subTypes")
    public void testDispatchRateLimitAtFirstDispatch(SubscriptionType subType) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        int ratePeriodInSecond = 60;
        int msgCountPerEntry = 10;
        int rateLimitMsgCountPerPeriod = 30;
        // Create topic, subscription, then send many messages.
        triggerTopicAndSubCreates(topicName, subName, subType);
        setSubscriptionDispatchRate(topicName, rateLimitMsgCountPerPeriod, -1, ratePeriodInSecond);
        sendManyBatchedMessages(msgCountPerEntry, 200, topicName);
        triggerManagedLedgerStatUpdate();

        // verify.
        int expectedReceiveEntryCount = rateLimitMsgCountPerPeriod;
        List<Consumer<String>> consumers = createConsumes(topicName, subName, 5, subType, 1000000);
        waitForReceivedEntryCount(consumers, Duration.ofSeconds(3),
                entryCount -> entryCount == expectedReceiveEntryCount);
        int alreadyReceivedEntryCount = expectedReceiveEntryCount;
        assertWillNotReceiveMessagesAnyMore(consumers, Duration.ofSeconds(3), alreadyReceivedEntryCount);

        // cleanup.
        closeConsumers(consumers);
        admin.topics().delete(topicName, false);
    }

    /**
     * After one active consumer received any messages, then the dispatcher rate limiter works correctly.
     */
    @Test(dataProvider = "subTypes")
    public void testDispatchRateLimitAfterFirstDispatch(SubscriptionType subType) throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        String subNameAnother = subName + "-another";
        int ratePeriodInSecond = 60;
        int msgCountPerEntry = 10;
        int rateLimitMsgCountPerPeriod = 30;

        // Create topic, subscription, rate limit rule.
        triggerTopicAndSubCreates(topicName, subName, subType);
        triggerTopicAndSubCreates(topicName, subNameAnother, subType);
        setSubscriptionDispatchRate(topicName, rateLimitMsgCountPerPeriod, -1, ratePeriodInSecond);

        // send many messages.
        sendManyBatchedMessages(msgCountPerEntry, 200, topicName);
        triggerManagedLedgerStatUpdate();

        // Let one active consumer(with another sub) know how many messages per batch.
        ConsumerKnownHowManyMessagesPerBatch consumerWithAnotherSub =
                letOneConsumerKnowAvgMsgPerEntryAndReceiveOneEntry(topicName, subNameAnother, subType);

        // verify.
        int expectedReceiveEntryCount = rateLimitMsgCountPerPeriod / msgCountPerEntry;
        List<Consumer<String>> consumers = createConsumes(topicName, subName, 10, subType, 1000000);
        waitForReceivedEntryCount(consumers, Duration.ofSeconds(3),
                entryCount -> entryCount == expectedReceiveEntryCount);
        int alreadyReceivedEntryCount = expectedReceiveEntryCount;
        assertWillNotReceiveMessagesAnyMore(consumers, Duration.ofSeconds(3), alreadyReceivedEntryCount);

        // cleanup.
        closeConsumers(consumerWithAnotherSub.consumer);
        closeConsumers(consumers);
        admin.topics().delete(topicName, false);
    }

    /**
     * Differ {@link #testDispatchRateLimitAfterFirstDispatch(SubscriptionType)}: just has only one subscription.
     */
    @Test
    public void testDispatchRateLimitAfterFirstDispatchInOneSub() throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        SubscriptionType subType = SubscriptionType.Shared;
        int ratePeriodInSecond = 60;
        int msgCountPerEntry = 10;
        int rateLimitMsgCountPerPeriod = 30;

        // Create topic, subscription, rate limit rule.
        triggerTopicAndSubCreates(topicName, subName, subType);
        setSubscriptionDispatchRate(topicName, rateLimitMsgCountPerPeriod, -1, ratePeriodInSecond);

        // send many messages.
        sendManyBatchedMessages(msgCountPerEntry, 200, topicName);
        triggerManagedLedgerStatUpdate();

        // Let one active consumer know how many messages per batch.
        ConsumerKnownHowManyMessagesPerBatch firstConsumer =
                letOneConsumerKnowAvgMsgPerEntryAndReceiveOneEntry(topicName, subName, subType);
        int firstConsumerReceivedEntryCount = firstConsumer.receivedEntries.size();

        // verify.
        int expectedReceiveEntryCount = rateLimitMsgCountPerPeriod / msgCountPerEntry - firstConsumerReceivedEntryCount;
        List<Consumer<String>> consumers = createConsumes(topicName, subName, 10, subType, 1000000);
        waitForReceivedEntryCount(consumers, Duration.ofSeconds(3),
                entryCount -> entryCount == expectedReceiveEntryCount);
        int alreadyReceivedEntryCount = expectedReceiveEntryCount;
        assertWillNotReceiveMessagesAnyMore(consumers, Duration.ofSeconds(3), alreadyReceivedEntryCount);

        // cleanup.
        closeConsumers(firstConsumer.consumer);
        closeConsumers(consumers);
        admin.topics().delete(topicName, false);
    }

    /**
     * If already reach the limit, new consumer can also receive messages in next period.
     */
    @Test
    public void testReadAfterReachLimit() throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        String subName = "sub";
        SubscriptionType subType = SubscriptionType.Shared;
        int ratePeriodInSecond = 2;
        int msgCountPerEntry = 10;
        int rateLimitMsgCountPerPeriod = 30;

        // Create topic, subscription, rate limit rule.
        Consumer<String> consumer1 =
                createConsumes(topicName, subName, 1, subType, 1000000).get(0);
        setSubscriptionDispatchRate(topicName, rateLimitMsgCountPerPeriod, -1, ratePeriodInSecond);

        // send many messages.
        sendManyBatchedMessages(msgCountPerEntry, 200, topicName);

        Consumer<String> consumer2 =
                createConsumes(topicName, subName, 1, subType, 1000000).get(0);
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Message<String> msg = consumer2.receive();
            assertNotNull(msg);
        });

        // cleanup.
        closeConsumers(consumer1, consumer2);
        admin.topics().delete(topicName, false);
    }

}
