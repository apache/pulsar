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

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.intercept.MockBrokerInterceptor;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.SharedPulsarCluster;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-api")
public class PersistentDispatcherSingleActiveConsumerTest extends SharedPulsarBaseTest {

    @Test
    public void testSkipReadEntriesFromCloseCursor() throws Exception {
        String topicName = newTopicName();
        String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        PersistentSubscription sub = topic.getSubscription(subscription);
        AtomicInteger scheduledReads = new AtomicInteger();
        PersistentDispatcherSingleActiveConsumer dispatcher =
                new PersistentDispatcherSingleActiveConsumer(sub.getCursor(),
                        CommandSubscribe.SubType.Exclusive, 0, topic, sub) {
                    @Override
                    void scheduleReadEntriesWithDelay(Consumer consumer, long delay) {
                        scheduledReads.incrementAndGet();
                        super.scheduleReadEntriesWithDelay(consumer, delay);
                    }
                };

        dispatcher.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("cursor closed"),
                null, 0);

        Assert.assertEquals(scheduledReads.get(), 0, "Closed cursor failures must not schedule another read");
        admin.topics().delete(topicName, false);
    }

    @Test
    public void testDispatchQuotaWhenSendMessageInfoReusedBeforeWriteCompletes() throws Exception {
        String topicName = newTopicName();
        String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        PersistentSubscription sub = topic.getSubscription(subscription);
        DispatchRateLimiter rateLimiter = mock(DispatchRateLimiter.class);
        PersistentDispatcherSingleActiveConsumer dispatcher =
                new PersistentDispatcherSingleActiveConsumer(sub.getCursor(),
                        CommandSubscribe.SubType.Exclusive, 0, topic, sub) {
                    @Override
                    public Optional<DispatchRateLimiter> getRateLimiter() {
                        return Optional.of(rateLimiter);
                    }
                };
        // The write to the consumer connection stays pending until the test completes it.
        Promise<Void> writePromise = ImmediateEventExecutor.INSTANCE.newPromise();
        Consumer consumer = mock(Consumer.class);
        when(consumer.sendMessages(any(), any(), any(), anyInt(), anyLong(), anyLong(), any(), anyLong()))
                .thenReturn(writePromise);

        SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
        sendMessageInfo.setTotalMessages(3);
        sendMessageInfo.setTotalBytes(300);
        dispatcher.dispatchEntriesToConsumer(consumer, List.of(mock(Entry.class)), EntryBatchSizes.get(1),
                EntryBatchIndexesAcks.get(1), sendMessageInfo, DEFAULT_CONSUMER_EPOCH);

        // Another dispatch on this thread reuses the thread-local SendMessageInfo before the write completes.
        SendMessageInfo nextSendMessageInfo = SendMessageInfo.getThreadLocal();
        nextSendMessageInfo.setTotalMessages(7);
        nextSendMessageInfo.setTotalBytes(700);
        writePromise.setSuccess(null);

        // The dispatch quota must be charged with the counts of the messages that were written.
        verify(rateLimiter).consumeDispatchQuota(3, 300);
    }

    @DataProvider
    public static Object[][] closeDelayMs() {
        return new Object[][] { { 500 }, { 2000 } };
    }

    @Test(dataProvider = "closeDelayMs")
    public void testOverrideInactiveConsumer(long closeDelayMs) throws Exception {
        BrokerService broker = SharedPulsarCluster.get().getPulsarService().getBrokerService();
        BrokerInterceptor previousInterceptor = broker.getInterceptor();
        final var interceptor = new Interceptor();
        broker.setInterceptor(interceptor);
        try {
            final var topic = newTopicName();
            @Cleanup final var client = PulsarClient.builder()
                    .serviceUrl(getBrokerServiceUrl()).build();
            @Cleanup final var consumer = client.newConsumer().topic(topic).subscriptionName("sub").subscribe();
            final var dispatcher = ((PersistentTopic) SharedPulsarCluster.get().getPulsarService().getBrokerService()
                    .getTopicIfExists(TopicName.get(topic).toString()).get().orElseThrow())
                    .getSubscription("sub").dispatcher;
            Assert.assertEquals(dispatcher.getConsumers().size(), 1);

            // Generally `isActive` could only be false after `channelInactive` is called; set it with false directly
            // to avoid race condition.
            final var latch = new CountDownLatch(1);
            interceptor.latch.set(latch);
            interceptor.injectCloseLatency.set(true);
            interceptor.delayMs = closeDelayMs;
            // Simulate the real case because `channelInactive` is always called in the event loop thread
            Consumer original = dispatcher.getConsumers().get(0);
            final var cnx = (ServerCnx) original.cnx();
            CompletableFuture<Void> connectionClosed = new CompletableFuture<>();
            cnx.ctx().executor().execute(() -> {
                try {
                    cnx.channelInactive(cnx.ctx());
                    connectionClosed.complete(null);
                } catch (Exception e) {
                    connectionClosed.completeExceptionally(e);
                }
            });

            Consumer replacement = new Consumer(original.getSubscription(), original.subType(), topic,
                    original.consumerId() + 1, 0, "replacement", true, cnx, "role", Collections.emptyMap(), false,
                    null, MessageId.latest, 0);
            try {
                Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
                if (closeDelayMs < 1000) {
                    dispatcher.addConsumer(replacement).get();
                    Assert.assertEquals(dispatcher.getConsumers().size(), 1);
                    Assert.assertSame(replacement, dispatcher.getConsumers().get(0));
                } else {
                    try {
                        dispatcher.addConsumer(replacement).get();
                        Assert.fail();
                    } catch (ExecutionException e) {
                        Assert.assertTrue(e.getCause() instanceof BrokerServiceException.ConsumerBusyException);
                    }
                }
            } finally {
                connectionClosed.get(10, TimeUnit.SECONDS);
                if (dispatcher.getConsumers().contains(replacement)) {
                    dispatcher.removeConsumer(replacement);
                }
            }
        } finally {
            broker.setInterceptor(previousInterceptor);
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
