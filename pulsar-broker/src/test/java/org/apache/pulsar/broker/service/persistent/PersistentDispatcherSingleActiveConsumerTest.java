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

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.intercept.MockBrokerInterceptor;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicName;
import org.mockito.Mockito;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class PersistentDispatcherSingleActiveConsumerTest extends ProducerConsumerBase {

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

    @Test
    public void testSkipReadEntriesFromCloseCursor() throws Exception {
        final String topicName =
                BrokerTestUtil.newUniqueName("persistent://public/default/testSkipReadEntriesFromCloseCursor");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        for (int i = 0; i < 10; i++) {
            producer.send("message-" + i);
        }
        producer.close();

        // Get the dispatcher of the topic.
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName, false).join().get();

        ManagedCursor cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn(subscription).when(cursor).getName();
        Subscription sub = Mockito.mock(PersistentSubscription.class);
        Mockito.doReturn(topic).when(sub).getTopic();
        // Mock the dispatcher.
        PersistentDispatcherSingleActiveConsumer dispatcher =
                Mockito.spy(new PersistentDispatcherSingleActiveConsumer(cursor, CommandSubscribe.SubType.Exclusive,0, topic, sub));

        // Mock a consumer
        Consumer consumer = Mockito.mock(Consumer.class);
        consumer.getAvailablePermits();
        Mockito.doReturn(10).when(consumer).getAvailablePermits();
        Mockito.doReturn(10).when(consumer).getAvgMessagesPerEntry();
        Mockito.doReturn("test").when(consumer).consumerName();
        Mockito.doReturn(true).when(consumer).isWritable();
        Mockito.doReturn(false).when(consumer).readCompacted();

        // Make the consumer as the active consumer.
        Mockito.doReturn(consumer).when(dispatcher).getActiveConsumer();

        // Make the count + 1 when call the scheduleReadEntriesWithDelay(...).
        AtomicInteger callScheduleReadEntriesWithDelayCnt = new AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            callScheduleReadEntriesWithDelayCnt.getAndIncrement();
            return inv.callRealMethod();
        }).when(dispatcher).scheduleReadEntriesWithDelay(Mockito.eq(consumer), Mockito.anyLong());

        // Make the count + 1 when call the readEntriesFailed(...).
        AtomicInteger callReadEntriesFailed = new AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            callReadEntriesFailed.getAndIncrement();
            return inv.callRealMethod();
        }).when(dispatcher).readEntriesFailed(Mockito.any(), Mockito.any());

        Mockito.doReturn(false).when(cursor).isClosed();

        // Mock the readEntriesOrWait(...) to simulate the cursor is closed.
        Mockito.doAnswer(inv -> {
            final var callback = (AsyncCallbacks.ReadEntriesCallback) inv.getArgument(2);
            callback.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("cursor closed"),
                    null);
            return null;
        }).when(cursor).asyncReadEntriesWithSkipOrWait(Mockito.anyInt(), Mockito.anyLong(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());

        dispatcher.readMoreEntries(consumer);

        // Verify: the readEntriesFailed should be called once and the scheduleReadEntriesWithDelay should not be called.
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(callReadEntriesFailed.get() == 1
                && callScheduleReadEntriesWithDelayCnt.get() == 0));

        // Verify: the topic can be deleted successfully.
        admin.topics().delete(topicName, false);
    }

    @DataProvider
    public static Object[][] closeDelayMs() {
        return new Object[][] { { 500 }, { 2000 } };
    }

    @Test(dataProvider = "closeDelayMs")
    public void testOverrideInactiveConsumer(long closeDelayMs) throws Exception {
        final var interceptor = new Interceptor();
        pulsar.getBrokerService().setInterceptor(interceptor);
        final var topic = "test-override-inactive-consumer-" + closeDelayMs;
        @Cleanup final var client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        @Cleanup final var consumer = client.newConsumer().topic(topic).subscriptionName("sub").subscribe();
        final var dispatcher = ((PersistentTopic) pulsar.getBrokerService().getTopicIfExists(TopicName.get(topic)
                .toString()).get().orElseThrow()).getSubscription("sub").dispatcher;
        Assert.assertEquals(dispatcher.getConsumers().size(), 1);

        // Generally `isActive` could only be false after `channelInactive` is called, setting it with false directly
        // to avoid race condition.
        final var latch = new CountDownLatch(1);
        interceptor.latch.set(latch);
        interceptor.injectCloseLatency.set(true);
        interceptor.delayMs = closeDelayMs;
        // Simulate the real case because `channelInactive` is always called in the event loop thread
        final var cnx = (ServerCnx) dispatcher.getConsumers().get(0).cnx();
        cnx.ctx().executor().execute(() -> {
            try {
                cnx.channelInactive(cnx.ctx());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        @Cleanup final var mockConsumer = Mockito.mock(Consumer.class);
        Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
        if (closeDelayMs < 1000) {
            dispatcher.addConsumer(mockConsumer).get();
            Assert.assertEquals(dispatcher.getConsumers().size(), 1);
            Assert.assertSame(mockConsumer, dispatcher.getConsumers().get(0));
        } else {
            try {
                dispatcher.addConsumer(mockConsumer).get();
                Assert.fail();
            } catch (ExecutionException e) {
                Assert.assertTrue(e.getCause() instanceof BrokerServiceException.ConsumerBusyException);
            }
        }
    }

    private static class Interceptor extends MockBrokerInterceptor {

        final AtomicBoolean injectCloseLatency = new AtomicBoolean(false);
        final AtomicReference<CountDownLatch> latch = new AtomicReference<>();
        long delayMs = 500;

        @Override
        public void onConnectionClosed(ServerCnx cnx) {
            if (injectCloseLatency.compareAndSet(true, false)) {
                Optional.ofNullable(latch.get()).ifPresent(CountDownLatch::countDown);
                latch.set(null);
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
