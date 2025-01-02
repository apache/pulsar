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
package org.apache.bookkeeper.mledger.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumersClassic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class NonEntryCacheKeySharedSubscriptionV30Test extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setManagedLedgerCacheSizeMB(0);
        this.conf.setManagedLedgerMaxEntriesPerLedger(50000);
        // Use the implementation of subscriptions in v3.x.
        this.conf.setSubscriptionKeySharedUseClassicPersistentImplementation(true);
    }


    @Test(timeOut = 180 * 1000, invocationCount = 1)
    public void testRecentJoinQueueIsInOrderAfterRewind() throws Exception {
        int msgCount = 300;
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subName = "my-sub";
        final DefaultThreadFactory threadFactory =
                new DefaultThreadFactory(BrokerTestUtil.newUniqueName("thread"));
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, subName, MessageId.earliest);

        // Send messages.
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32).topic(topic).enableBatching(false).create();
        AtomicInteger msgGenerator = new AtomicInteger();
        for (int i = 0; i < msgCount; i++) {
            int v = msgGenerator.getAndIncrement();
            producer.newMessage().key(String.valueOf(v)).value(v).send();
        }

        // Make ack holes.
        Consumer<Integer> consumer1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(100)
                .subscriptionType(SubscriptionType.Key_Shared)
                .consumerName("c1")
                .subscribe();
        Consumer<Integer> consumer2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(100)
                .subscriptionType(SubscriptionType.Key_Shared)
                .consumerName("c2")
                .subscribe();
        Consumer<Integer> consumer3 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(100)
                .subscriptionType(SubscriptionType.Key_Shared)
                .consumerName("c3")
                .subscribe();
        final PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        final ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        final PersistentSubscription persistentSubscription = persistentTopic.getSubscription(subName);
        final PersistentStickyKeyDispatcherMultipleConsumersClassic dispatcher =
                (PersistentStickyKeyDispatcherMultipleConsumersClassic) persistentSubscription.getDispatcher();
        final ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(subName);
        dispatcher.setSortRecentlyJoinedConsumersIfNeeded(false);

        // Make ack holes.
        //  - ack all messages that consumer1 or consumer2 received.
        //  - do not ack messages that consumer2 received.
        ackAllMessages(consumer1, consumer2);
        Position mdPosition = (Position) cursor.getMarkDeletedPosition();
        Position readPosition = (Position) cursor.getReadPosition();
        Position LAC = (Position) ml.getLastConfirmedEntry();
        assertTrue(readPosition.compareTo(LAC) >= 0);
        Position firstWaitingAckPos = ml.getNextValidPosition(mdPosition);
        log.info("md-pos {}:{}", mdPosition.getLedgerId(), mdPosition.getEntryId());
        log.info("rd-pos {}:{}", readPosition.getLedgerId(), readPosition.getEntryId());
        log.info("lac-pos {}:{}", LAC.getLedgerId(), LAC.getEntryId());
        log.info("first-waiting-ack-pos {}:{}", firstWaitingAckPos.getLedgerId(), firstWaitingAckPos.getEntryId());

        // Inject a delay for the next replay read.
        LedgerHandle firstLedger = ml.currentLedger;
        Assert.assertEquals(firstWaitingAckPos.getLedgerId(), firstLedger.getId());
        LedgerHandle spyFirstLedger = spy(firstLedger);
        CountDownLatch replyReadSignal = new CountDownLatch(1);
        AtomicBoolean replayReadWasTriggered = new AtomicBoolean();
        Answer answer = invocation -> {
            long firstEntry = (long) invocation.getArguments()[0];
            if (firstEntry == firstWaitingAckPos.getEntryId()) {
                replayReadWasTriggered.set(true);
                final CompletableFuture res = new CompletableFuture<>();
                threadFactory.newThread(() -> {
                    try {
                        replyReadSignal.await();
                        CompletableFuture<LedgerEntries> future =
                                (CompletableFuture<LedgerEntries>) invocation.callRealMethod();
                        future.thenAccept(v -> {
                            res.complete(v);
                        }).exceptionally(ex -> {
                            res.completeExceptionally(ex);
                            return null;
                        });
                    } catch (Throwable ex) {
                        res.completeExceptionally(ex);
                    }
                }).start();
                return res;
            } else {
                return invocation.callRealMethod();
            }
        };
        doAnswer(answer).when(spyFirstLedger).readAsync(anyLong(), anyLong());
        doAnswer(answer).when(spyFirstLedger).readUnconfirmedAsync(anyLong(), anyLong());
        ml.currentLedger = spyFirstLedger;

        // Keep publish to avoid pending normal read.
        AtomicBoolean keepPublishing = new AtomicBoolean(true);
        new Thread(() -> {
            while (keepPublishing.get()) {
                int v = msgGenerator.getAndIncrement();
                producer.newMessage().key(String.valueOf(v)).value(v).sendAsync();
                sleep(100);
            }
        }).start();

        // Trigger a message redelivery.
        consumer3.close();
        Awaitility.await().until(() -> replayReadWasTriggered.get());

        // Close all consumers to trigger a cursor.rewind.
        consumer1.close();
        consumer2.close();

        // Start 100 consumers.
        List<CompletableFuture<Consumer<Integer>>> consumerList = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            consumerList.add(pulsarClient.newConsumer(Schema.INT32)
                    .topic(topic)
                    .subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscribeAsync());
            if (i == 10) {
                for (int j = 0; j < msgCount; j++) {
                    int v = msgGenerator.getAndIncrement();
                    producer.newMessage().key(String.valueOf(v)).value(v).send();
                }
                final Consumer<Integer> firstConsumer = consumerList.get(0).join();
                ackAllMessages(firstConsumer);
                new Thread(() -> {
                    while (keepPublishing.get()) {
                        try {
                            ackAllMessages(firstConsumer);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }).start();
            }
            log.info("recent-joined-consumers {} {}", i, dispatcher.getRecentlyJoinedConsumers().size());
            if (dispatcher.getRecentlyJoinedConsumers().size() > 0) {
                Position mdPosition2 = (Position) cursor.getMarkDeletedPosition();
                Position readPosition2 = (Position) cursor.getReadPosition();
                Position LAC2 = (Position) ml.getLastConfirmedEntry();
                assertTrue(readPosition.compareTo(LAC) >= 0);
                Position firstWaitingAckPos2 = ml.getNextValidPosition(mdPosition);
                if(readPosition2.compareTo(firstWaitingAckPos) > 0) {
                    keepPublishing.set(false);
                    log.info("consumer-index: {}", i);
                    log.info("md-pos-2 {}:{}", mdPosition2.getLedgerId(), mdPosition2.getEntryId());
                    log.info("rd-pos-2 {}:{}", readPosition2.getLedgerId(), readPosition2.getEntryId());
                    log.info("lac-pos-2 {}:{}", LAC2.getLedgerId(), LAC2.getEntryId());
                    log.info("first-waiting-ack-pos-2 {}:{}", firstWaitingAckPos2.getLedgerId(),
                            firstWaitingAckPos2.getEntryId());
                    // finish the replay read here.
                    replyReadSignal.countDown();
                } else {
                    sleep(1000);
                }
            }
        }
        consumerList.get(consumerList.size() - 1).join();

        synchronized (dispatcher) {
            LinkedHashMap recentJoinedConsumers = dispatcher.getRecentlyJoinedConsumers();
            assertTrue(verifyMapItemsAreInOrder(recentJoinedConsumers));
        }

        // cleanup.
        producer.close();
        for (CompletableFuture<Consumer<Integer>> c : consumerList) {
            c.join().close();
        }
        admin.topics().delete(topic, false);
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean verifyMapItemsAreInOrder(LinkedHashMap<org.apache.pulsar.broker.service.Consumer, Position> map) {
        boolean outOfOrder = false;
        Position posPre = null;
        Position posAfter = null;
        for (Map.Entry<org.apache.pulsar.broker.service.Consumer, Position> entry : map.entrySet()) {
            if (posPre == null) {
                posPre = (Position) entry.getValue();
            } else {
                posAfter = (Position) entry.getValue();
            }
            if (posPre != null && posAfter != null) {
                if (posPre.compareTo(posAfter) > 0) {
                    outOfOrder = true;
                    break;
                }
                posPre = posAfter;
            }
        }
        return !outOfOrder;
    }
}
