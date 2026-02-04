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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Deterministic regression test for GitHub issue #25204: MultiTopicsConsumer can deliver one message
 * with old consumerEpoch while other messages in the same batch are filtered, due to a race when
 * redeliverUnacknowledgedMessages() runs between batch message iterations.
 *
 * <p>Uses a test-only subclass of MultiTopicsConsumerImpl that overrides isValidConsumerEpoch to
 * block after the first validation (latch1/latch2). With a 2-thread internal executor, the test
 * triggers redeliverUnacknowledgedMessages() while the receive thread is blocked between the two
 * validations, so the epoch changes mid-batch. Without the fix, exactly one message is delivered
 * (bug). The test asserts totalDelivered != 1.
 */
@Test(groups = "flaky")
public class MultiTopicsConsumerEpochRaceTest {

    private static final String TOPIC_NAME = "topic-25204";
    private static final int AWAIT_SECONDS = 15;

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private PulsarClientImpl clientMock;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        executorProvider = new ExecutorProvider(2, "MultiTopicsConsumerEpochRaceTest");
        internalExecutor = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("internal-executor", true));
        clientMock = ClientTestFixtures.createPulsarClientMockWithMockedClientCnx(
                executorProvider, internalExecutor);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanUp() {
        if (executorProvider != null) {
            executorProvider.shutdownNow();
        }
        if (internalExecutor != null) {
            internalExecutor.shutdownNow();
        }
    }

    /**
     * Deterministic reproduction: test subclass blocks inside isValidConsumerEpoch on the first
     * call; test thread triggers redeliver (epoch increments on another executor thread), then
     * unblocks. Second message is validated with new epoch and filtered. Outcome: exactly one
     * message delivered. Assertion: totalDelivered must not be 1.
     */
    @Test(timeOut = 30000, description = "Issue 25204: batch must not have exactly one message "
            + "delivered when epoch changes mid-batch (deterministic race)")
    @SuppressWarnings("unchecked")
    public void testEpochConsistencyWhenRedeliverRunsMidBatch() throws Exception {
        CountDownLatch afterFirstValidationLatch = new CountDownLatch(1);
        CountDownLatch releaseValidationLatch = new CountDownLatch(1);
        CountDownLatch redeliverDoneLatch = new CountDownLatch(1);
        CountDownLatch forEachCompleteLatch = new CountDownLatch(1);
        AtomicInteger validationCallCount = new AtomicInteger(0);
        AtomicInteger listenerInvocationCount = new AtomicInteger(0);
        AtomicInteger acceptedByEpochCount = new AtomicInteger(0);

        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setTopicNames(Collections.emptySet());
        conf.setSubscriptionName("sub-25204");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setReceiverQueueSize(10);
        MessageListener<byte[]> listener = (consumer, msg) -> listenerInvocationCount.incrementAndGet();
        conf.setMessageListener(listener);

        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
        TestableMultiTopicsConsumerImpl multiConsumer = new TestableMultiTopicsConsumerImpl(
                clientMock, conf, executorProvider, subscribeFuture, Schema.BYTES, null, true);
        multiConsumer.setEpochRaceTestLatches(
                afterFirstValidationLatch, releaseValidationLatch, redeliverDoneLatch,
                forEachCompleteLatch, validationCallCount, acceptedByEpochCount);

        subscribeFuture.get(AWAIT_SECONDS, TimeUnit.SECONDS);
        assertTrue(multiConsumer.getConsumers().isEmpty(), "Empty topic set gives 0 consumers");

        ConsumerImpl<byte[]> mockConsumer = mock(ConsumerImpl.class);
        when(mockConsumer.getTopic()).thenReturn(TOPIC_NAME);
        doNothing().when(mockConsumer).increaseAvailablePermits(any(ClientCnx.class));
        ClientCnx cnxMock = mock(ClientCnx.class);
        org.apache.pulsar.client.impl.ConnectionHandler connectionHandlerMock =
                mock(org.apache.pulsar.client.impl.ConnectionHandler.class);
        when(connectionHandlerMock.cnx()).thenReturn(cnxMock);
        when(mockConsumer.getConnectionHandler()).thenReturn(connectionHandlerMock);
        when(mockConsumer.getCurrentReceiverQueueSize()).thenReturn(10);

        CompletableFuture<Messages<byte[]>> batchFuture = new CompletableFuture<>();
        when(mockConsumer.batchReceiveAsync()).thenReturn(batchFuture);

        Field consumersField = MultiTopicsConsumerImpl.class.getDeclaredField("consumers");
        consumersField.setAccessible(true);
        java.util.Map<String, ConsumerImpl<byte[]>> consumersMap =
                (java.util.Map<String, ConsumerImpl<byte[]>>) consumersField.get(multiConsumer);
        consumersMap.put(TOPIC_NAME, mockConsumer);

        Method startReceiving = MultiTopicsConsumerImpl.class.getDeclaredMethod(
                "startReceivingMessages", List.class);
        startReceiving.setAccessible(true);
        startReceiving.invoke(multiConsumer, Collections.singletonList(mockConsumer));

        MessageImpl<byte[]> msg1 = createMessageWithEpoch(0);
        MessageImpl<byte[]> msg2 = createMessageWithEpoch(0);
        MessagesImpl<byte[]> messages = new MessagesImpl<>(10, 10_000);
        messages.add(msg1);
        messages.add(msg2);

        batchFuture.complete(messages);

        assertTrue(afterFirstValidationLatch.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                "Receive thread should block after first validation");
        Thread.sleep(200);

        multiConsumer.redeliverUnacknowledgedMessages();
        assertTrue(redeliverDoneLatch.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                "Redeliver runnable should complete");
        releaseValidationLatch.countDown();

        assertTrue(forEachCompleteLatch.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                "Receive callback forEach should complete");

        int acceptedByEpoch = acceptedByEpochCount.get();
        assertNotEquals(acceptedByEpoch, 1,
                "Bug 25204: batch must not have exactly one message accepted by epoch when "
                        + "epoch changes mid-batch (acceptedByEpoch=" + acceptedByEpoch
                        + "; expected 0 or 2, not 1)");
    }

    private static MessageImpl<byte[]> createMessageWithEpoch(long consumerEpoch) throws Exception {
        MessageMetadata meta = new MessageMetadata();
        MessageImpl<byte[]> msg = MessageImpl.create(meta, ByteBuffer.allocate(0), Schema.BYTES, null);
        msg.setMessageId(new MessageIdImpl(1, 0, -1));
        Field epochField = MessageImpl.class.getDeclaredField("consumerEpoch");
        epochField.setAccessible(true);
        epochField.setLong(msg, consumerEpoch);
        return msg;
    }

    /**
     * Test-only subclass that blocks inside the batch loop after the first epoch validation,
     * so the test can run redeliverUnacknowledgedMessages() and increment the epoch before
     * the second message is validated. Overrides isValidConsumerEpoch (epoch validation hook)
     * and redeliverUnacknowledgedMessages (to signal when redeliver has run).
     */
    static class TestableMultiTopicsConsumerImpl extends MultiTopicsConsumerImpl<byte[]> {

        private CountDownLatch afterFirstValidationLatch;
        private CountDownLatch releaseValidationLatch;
        private CountDownLatch redeliverDoneLatch;
        private CountDownLatch forEachCompleteLatch;
        private AtomicInteger validationCallCount;
        private AtomicInteger acceptedByEpochCount;

        TestableMultiTopicsConsumerImpl(PulsarClientImpl client,
                ConsumerConfigurationData<byte[]> conf,
                ExecutorProvider executorProvider,
                CompletableFuture<Consumer<byte[]>> subscribeFuture,
                Schema<byte[]> schema,
                ConsumerInterceptors<byte[]> interceptors,
                boolean createTopicIfDoesNotExist) {
            super(client, conf, executorProvider, subscribeFuture, schema, interceptors,
                    createTopicIfDoesNotExist);
        }

        void setEpochRaceTestLatches(CountDownLatch afterFirstValidationLatch,
                CountDownLatch releaseValidationLatch,
                CountDownLatch redeliverDoneLatch,
                CountDownLatch forEachCompleteLatch,
                AtomicInteger validationCallCount,
                AtomicInteger acceptedByEpochCount) {
            this.afterFirstValidationLatch = afterFirstValidationLatch;
            this.releaseValidationLatch = releaseValidationLatch;
            this.redeliverDoneLatch = redeliverDoneLatch;
            this.forEachCompleteLatch = forEachCompleteLatch;
            this.validationCallCount = validationCallCount;
            this.acceptedByEpochCount = acceptedByEpochCount;
        }

        @Override
        protected boolean isValidConsumerEpoch(MessageImpl<byte[]> message) {
            int callIndex = validationCallCount != null ? validationCallCount.getAndIncrement() : -1;
            if (afterFirstValidationLatch != null && releaseValidationLatch != null
                    && forEachCompleteLatch != null && validationCallCount != null && callIndex == 0) {
                boolean result = super.isValidConsumerEpoch(message);
                if (result && acceptedByEpochCount != null) {
                    acceptedByEpochCount.incrementAndGet();
                }
                afterFirstValidationLatch.countDown();
                try {
                    assertTrue(releaseValidationLatch.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                            "Test must release validation latch within timeout");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return result;
            }
            boolean result = super.isValidConsumerEpoch(message);
            if (result && acceptedByEpochCount != null) {
                acceptedByEpochCount.incrementAndGet();
            }
            if (forEachCompleteLatch != null && callIndex == 1) {
                forEachCompleteLatch.countDown();
            }
            return result;
        }

        @Override
        public void redeliverUnacknowledgedMessages() {
            internalPinnedExecutor.execute(() -> {
                try {
                    incomingQueueLock.lock();
                    try {
                        CONSUMER_EPOCH.incrementAndGet(this);
                        consumers.values().stream().forEach(consumer -> {
                            consumer.redeliverUnacknowledgedMessages();
                            if (consumer.unAckedChunkedMessageIdSequenceMap != null) {
                                consumer.unAckedChunkedMessageIdSequenceMap.clear();
                            }
                        });
                        clearIncomingMessages();
                        unAckedMessageTracker.clear();
                        try {
                            Method resumeReceiving = MultiTopicsConsumerImpl.class.getDeclaredMethod(
                                    "resumeReceivingFromPausedConsumersIfNeeded");
                            resumeReceiving.setAccessible(true);
                            resumeReceiving.invoke(this);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } finally {
                        incomingQueueLock.unlock();
                    }
                } finally {
                    if (redeliverDoneLatch != null) {
                        redeliverDoneLatch.countDown();
                    }
                }
            });
        }
    }
}
