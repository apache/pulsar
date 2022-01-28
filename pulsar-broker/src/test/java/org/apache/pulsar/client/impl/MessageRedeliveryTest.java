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

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Cleanup;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import io.netty.util.concurrent.DefaultThreadFactory;

@Test(groups = "broker-impl")
public class MessageRedeliveryTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MessageRedeliveryTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "useOpenRangeSet")
    public static Object[][] useOpenRangeSet() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    /**
     * It tests that ManagedCursor tracks individually deleted messages and markDeletePosition correctly with different
     * range-set implementation and re-delivers messages as expected.
     *
     * @param useOpenRangeSet
     * @throws Exception
     */
    @Test(dataProvider = "useOpenRangeSet", timeOut = 30000)
    public void testRedelivery(boolean useOpenRangeSet) throws Exception {

        this.conf.setManagedLedgerMaxEntriesPerLedger(5);
        this.conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        this.conf.setManagedLedgerUnackedRangesOpenCacheSetEnabled(useOpenRangeSet);
        @Cleanup("shutdownNow")
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
                new DefaultThreadFactory("pulsar"));
        final String ns1 = "my-property/brok-ns1";
        final String subName = "my-subscriber-name";
        final int numMessages = 50;
        admin.namespaces().createNamespace(ns1, Sets.newHashSet("test"));

        final String topic1 = "persistent://" + ns1 + "/my-topic";

        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic1)
                .subscriptionName(subName).subscriptionType(SubscriptionType.Shared).receiverQueueSize(10)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic1)
                .subscriptionName(subName).subscriptionType(SubscriptionType.Shared).receiverQueueSize(10)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topic1).create();

        for (int i = 0; i < numMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        CountDownLatch latch = new CountDownLatch(numMessages);
        AtomicBoolean consume1 = new AtomicBoolean(true);
        AtomicBoolean consume2 = new AtomicBoolean(true);
        Set<String> ackedMessages = Sets.newConcurrentHashSet();
        AtomicInteger counter = new AtomicInteger(0);

        // (1) ack alternate message from consumer-1 which creates ack-hole.
        executor.submit(() -> {
            while (true) {
                try {
                    Message<byte[]> msg = consumer1.receive(1000, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        if (counter.getAndIncrement() % 2 == 0) {
                            try {
                                consumer1.acknowledge(msg);
                                // ack alternate messages
                                ackedMessages.add(new String(msg.getData()));
                            } catch (PulsarClientException e1) {
                                log.warn("Failed to ack message {}", e1.getMessage());
                            }
                        }
                    } else {
                        break;
                    }
                } catch (PulsarClientException e2) {
                    // Ok
                    break;
                }
                latch.countDown();
            }
        });

        // (2) ack all the consumed messages from consumer-2
        executor.submit(() -> {
            while (consume2.get()) {
                try {
                    Message<byte[]> msg = consumer2.receive(1000, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        consumer2.acknowledge(msg);
                        // ack alternate messages
                        ackedMessages.add(new String(msg.getData()));
                    } else {
                        break;
                    }
                } catch (PulsarClientException e2) {
                    // Ok
                    break;
                }
                latch.countDown();
            }
        });

        latch.await(10000, TimeUnit.MILLISECONDS);
        consume1.set(false);

        // (3) sleep so, consumer2 should timeout on it's pending read operation and not consume more messages
        Thread.sleep(1000);

        // (4) here we consume all messages but consumer1 only acked alternate messages.
        assertNotEquals(ackedMessages.size(), numMessages);

        PersistentTopic pTopic = (PersistentTopic) this.pulsar.getBrokerService().getTopicIfExists(topic1).get()
                .get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) pTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().iterator().next();

        // (5) now, close consumer1 and let broker deliver consumer1's unack messages to consumer2
        consumer1.close();

        // (6) broker should redeliver all unack messages of consumer-1 and consumer-2 should ack all of them
        CountDownLatch latch2 = new CountDownLatch(1);
        executor.submit(() -> {
            while (true) {
                try {
                    Message<byte[]> msg = consumer2.receive(1000, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        consumer2.acknowledge(msg);
                        // ack alternate messages
                        ackedMessages.add(new String(msg.getData()));
                    } else {
                        break;
                    }
                } catch (PulsarClientException e2) {
                    // Ok
                    break;
                }
                if (ackedMessages.size() == numMessages)
                    latch2.countDown();
            }

        });

        latch2.await(20000, TimeUnit.MILLISECONDS);

        consumer2.close();

        assertEquals(ackedMessages.size(), numMessages);

        // (7) acked message set should be empty
        assertEquals(cursor.getIndividuallyDeletedMessagesSet().size(), 0);

        // markDelete position should be one position behind read position
        assertEquals(cursor.getReadPosition(), cursor.getMarkDeletedPosition().getNext());

        producer.close();
        consumer2.close();


    }

    @Test
    public void testDoNotRedeliveryMarkDeleteMessages() throws PulsarClientException, PulsarAdminException {
        final String topic = "testDoNotRedeliveryMarkDeleteMessages";
        final String subName = "my-sub";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();

        producer.send("Pulsar".getBytes());

        for (int i = 0; i < 2; i++) {
            Message message = consumer.receive();
            assertNotNull(message);
        }

        admin.topics().skipAllMessages(topic, subName);

        Message message = null;

        try {
            message = consumer.receive(2, TimeUnit.SECONDS);
        } catch (Exception ignore) {
        }

        assertNull(message);
    }

}
