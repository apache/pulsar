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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Cleanup;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ClientErrorsTest {

    MockBrokerService mockBrokerService;
    private static final int ASYNC_EVENT_COMPLETION_WAIT = 100;

    private final String ASSERTION_ERROR = "AssertionError";

    @BeforeClass(alwaysRun = true)
    public void setup() {
        mockBrokerService = new MockBrokerService();
        mockBrokerService.start();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        if (mockBrokerService != null) {
            mockBrokerService.stop();
        }
    }

    @Test
    public void testMockBrokerService() throws PulsarClientException {
        // test default actions of mock broker service
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        try {
            Consumer<byte[]> consumer = client.newConsumer().topic("persistent://prop/use/ns/t1")
                    .subscriptionName("sub1").subscribe();

            Producer<byte[]> producer = client.newProducer().topic("persistent://prop/use/ns/t1").create();
            Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
            producer.send("message".getBytes());
            Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

            consumer.unsubscribe();

            producer.close();
            consumer.close();
        } catch (Exception e) {
            fail("None of the mocked operations should throw a client side exception");
        }
    }

    @Test
    public void testProducerCreateFailWithoutRetry() throws Exception {
        producerCreateFailWithoutRetry("persistent://prop/use/ns/t1");
    }

    @Test
    public void testPartitionedProducerCreateFailWithoutRetry() throws Exception {
        producerCreateFailWithoutRetry("persistent://prop/use/ns/part-t1");
    }

    private void producerCreateFailWithoutRetry(String topic) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (counter.incrementAndGet() == 2) {
                // piggyback unknown error to relay assertion failure
                ctx.writeAndFlush(
                        Commands.newError(producer.getRequestId(), ServerError.UnknownError, ASSERTION_ERROR));
                return;
            }
            ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.AuthorizationError, "msg"));
        });

        try {
            client.newProducer().topic(topic).create();
        } catch (Exception e) {
            if (e.getMessage().equals(ASSERTION_ERROR)) {
                fail("Producer create should not retry on auth error");
            }
            assertTrue(e instanceof PulsarClientException.AuthorizationException);
        }
        mockBrokerService.resetHandleProducer();
    }

    @Test
    public void testProducerCreateSuccessAfterRetry() throws Exception {
        producerCreateSuccessAfterRetry("persistent://prop/use/ns/t1");
    }

    @Test
    public void testPartitionedProducerCreateSuccessAfterRetry() throws Exception {
        producerCreateSuccessAfterRetry("persistent://prop/use/ns/part-t1");
    }

    private void producerCreateSuccessAfterRetry(String topic) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (counter.incrementAndGet() == 2) {
                ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer", SchemaVersion.Empty));
                return;
            }
            ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.ServiceNotReady, "msg"));
        });

        try {
            client.newProducer().topic(topic).create();
        } catch (Exception e) {
            fail("Should not fail");
        }

        mockBrokerService.resetHandleProducer();
    }

    @Test
    public void testProducerCreateFailAfterRetryTimeout() throws Exception {
        producerCreateFailAfterRetryTimeout("persistent://prop/use/ns/t1");
    }

    @Test
    public void testPartitionedProducerCreateFailAfterRetryTimeout() throws Exception {
        producerCreateFailAfterRetryTimeout("persistent://prop/use/ns/part-t1");
    }

    private void producerCreateFailAfterRetryTimeout(String topic) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress())
                .operationTimeout(1, TimeUnit.SECONDS).build();
        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicInteger closeProducerCounter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (counter.incrementAndGet() == 2) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
            ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.ServiceNotReady, "msg"));
        });

        mockBrokerService.setHandleCloseProducer((ctx, closeProducer) -> {
            closeProducerCounter.incrementAndGet();
        });

        try {
            client.newProducer().topic(topic).create();
            fail("Should have failed");
        } catch (Exception e) {
            // we fail even on the retriable error
            assertTrue(e instanceof PulsarClientException);
        }
        // There is a small race condition here because the producer's timeout both fails the client creation
        // and triggers sending CloseProducer.
        Awaitility.await().until(() -> closeProducerCounter.get() == 1);
        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleCloseProducer();
    }

    @Test
    public void testCreatedProducerSendsCloseProducerAfterTimeout() throws Exception {
        producerCreatedThenFailsRetryTimeout("persistent://prop/use/ns/t1");
    }

    @Test
    public void testCreatedPartitionedProducerSendsCloseProducerAfterTimeout() throws Exception {
        producerCreatedThenFailsRetryTimeout("persistent://prop/use/ns/part-t1");
    }

    private void producerCreatedThenFailsRetryTimeout(String topic) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress())
                .operationTimeout(1, TimeUnit.SECONDS).build();
        final AtomicInteger producerCounter = new AtomicInteger(0);
        final AtomicInteger closeProducerCounter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            int producerCount = producerCounter.incrementAndGet();
            if (producerCount == 1) {
                ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "producer1",
                        SchemaVersion.Empty));
                // Trigger reconnect
                ctx.writeAndFlush(Commands.newCloseProducer(producer.getProducerId(), -1));
            } else if (producerCount != 2) {
                // Respond to subsequent requests to prevent timeouts
                ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "producer1",
                        SchemaVersion.Empty));
            }
            // Don't respond to the second Producer command to ensure timeout
        });

        mockBrokerService.setHandleCloseProducer((ctx, closeProducer) -> {
            closeProducerCounter.incrementAndGet();
            ctx.writeAndFlush(Commands.newSuccess(closeProducer.getRequestId()));
        });

        // Create producer should succeed then upon closure, it should reattempt creation. The first request will
        // timeout, which triggers CloseProducer. The client might send send the third Producer command before the
        // below assertion, so we pass with 2 or 3.
        client.newProducer().topic(topic).create();
        Awaitility.await().until(() -> closeProducerCounter.get() == 1);
        Awaitility.await().until(() -> producerCounter.get() == 2 || producerCounter.get() == 3);
        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleCloseProducer();
    }

    @Test
    public void testProducerFailDoesNotFailOtherProducer() throws Exception {
        producerFailDoesNotFailOtherProducer("persistent://prop/use/ns/t1", "persistent://prop/use/ns/t2");
    }

    @Test
    public void testPartitionedProducerFailDoesNotFailOtherProducer() throws Exception {
        producerFailDoesNotFailOtherProducer("persistent://prop/use/ns/part-t1", "persistent://prop/use/ns/part-t2");
    }

    private void producerFailDoesNotFailOtherProducer(String topic1, String topic2) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (counter.incrementAndGet() == 2) {
                // fail second producer
                ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.AuthorizationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer", SchemaVersion.Empty));

        });

        ProducerBase<byte[]> producer1 = (ProducerBase<byte[]>) client.newProducer().topic(topic1).create();

        ProducerBase<byte[]> producer2 = null;
        try {
            producer2 = (ProducerBase<byte[]>) client.newProducer().topic(topic2).create();
            fail("Should have failed");
        } catch (Exception e) {
            // ok
        }

        assertTrue(producer1.isConnected());
        assertFalse(producer2 != null && producer2.isConnected());

        mockBrokerService.resetHandleProducer();
    }

    @Test
    public void testProducerContinuousRetryAfterSendFail() throws Exception {
        producerContinuousRetryAfterSendFail("persistent://prop/use/ns/t1");
    }

    @Test
    public void testPartitionedProducerContinuousRetryAfterSendFail() throws Exception {
        producerContinuousRetryAfterSendFail("persistent://prop/use/ns/part-t1");
    }

    private void producerContinuousRetryAfterSendFail(String topic) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            int i = counter.incrementAndGet();
            if (i == 1 || i == 5) {
                // succeed on 1st and 5th attempts
                ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer", SchemaVersion.Empty));
                return;
            }
            ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.PersistenceError, "msg"));
        });

        final AtomicInteger msgCounter = new AtomicInteger(0);
        mockBrokerService.setHandleSend((ctx, send, headersAndPayload) -> {
            // fail send once, but succeed later
            if (msgCounter.incrementAndGet() == 1) {
                ctx.writeAndFlush(Commands.newSendError(0, 0, ServerError.PersistenceError, "Send Failed"));
                return;
            }
            ctx.writeAndFlush(Commands.newSendReceipt(0, 0, 0, 1, 1));
        });

        try {
            Producer<byte[]> producer = client.newProducer().topic(topic).create();
            producer.send("message".getBytes());
        } catch (Exception e) {
            fail("Should not fail");
        }

        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleSend();
    }

    @Test
    public void testSubscribeFailWithoutRetry() throws Exception {
        subscribeFailWithoutRetry("persistent://prop/use/ns/t1");
    }

    @Test
    public void testPartitionedSubscribeFailWithoutRetry() throws Exception {
        subscribeFailWithoutRetry("persistent://prop/use/ns/part-t1");
    }

    @Test
    public void testLookupWithDisconnection() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        final AtomicInteger counter = new AtomicInteger(0);
        String topic = "persistent://prop/use/ns/t1";

        mockBrokerService.setHandlePartitionLookup((ctx, lookup) -> {
            ctx.writeAndFlush(Commands.newPartitionMetadataResponse(0, lookup.getRequestId()));
        });

        mockBrokerService.setHandleLookup((ctx, lookup) -> {
            if (counter.incrementAndGet() == 1) {
                // piggyback unknown error to relay assertion failure
                ctx.close();
                return;
            }
            ctx.writeAndFlush(
                    Commands.newLookupResponse(mockBrokerService.getBrokerAddress(), null, true, LookupType.Connect,
                            lookup.getRequestId(), false));
        });

        try {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe();
        } catch (Exception e) {
            if (e.getMessage().equals(ASSERTION_ERROR)) {
                fail("Subscribe should not retry on persistence error");
            }
            assertTrue(e instanceof PulsarClientException.BrokerPersistenceException);
        }
        mockBrokerService.resetHandlePartitionLookup();
        mockBrokerService.resetHandleLookup();

    }

    private void subscribeFailWithoutRetry(String topic) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress())
                .operationTimeout(1, TimeUnit.SECONDS).build();
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            if (counter.incrementAndGet() == 2) {
                // piggyback unknown error to relay assertion failure
                ctx.writeAndFlush(
                        Commands.newError(subscribe.getRequestId(), ServerError.UnknownError, ASSERTION_ERROR));
                return;
            }
            ctx.writeAndFlush(Commands.newError(subscribe.getRequestId(), ServerError.PersistenceError, "msg"));
        });

        try {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe();
        } catch (Exception e) {
            if (e.getMessage().equals(ASSERTION_ERROR)) {
                fail("Subscribe should not retry on persistence error");
            }
            assertTrue(e instanceof PulsarClientException.BrokerPersistenceException);
        }
        mockBrokerService.resetHandleSubscribe();
    }

    @Test
    public void testSubscribeSuccessAfterRetry() throws Exception {
        subscribeSuccessAfterRetry("persistent://prop/use/ns/t1");
    }

    @Test
    public void testPartitionedSubscribeSuccessAfterRetry() throws Exception {
        subscribeSuccessAfterRetry("persistent://prop/use/ns/part-t1");
    }

    private void subscribeSuccessAfterRetry(String topic) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            if (counter.incrementAndGet() == 2) {
                ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));
                return;
            }
            ctx.writeAndFlush(Commands.newError(subscribe.getRequestId(), ServerError.ServiceNotReady, "msg"));
        });

        try {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe();
        } catch (Exception e) {
            fail("Should not fail");
        }

        mockBrokerService.resetHandleSubscribe();
    }

    @Test
    public void testSubscribeFailAfterRetryTimeout() throws Exception {
        subscribeFailAfterRetryTimeout("persistent://prop/use/ns/t1");
    }

    @Test
    public void testPartitionedSubscribeFailAfterRetryTimeout() throws Exception {
        subscribeFailAfterRetryTimeout("persistent://prop/use/ns/part-t1");
    }

    private void subscribeFailAfterRetryTimeout(String topic) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress())
                .operationTimeout(200, TimeUnit.MILLISECONDS).build();
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            if (counter.incrementAndGet() == 2) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
            ctx.writeAndFlush(Commands.newError(subscribe.getRequestId(), ServerError.ServiceNotReady, "msg"));
        });

        try {
            client.newConsumer().topic(topic).subscriptionName("sub1").subscribe();
            fail("Should have failed");
        } catch (Exception e) {
            // we fail even on the retriable error
            assertTrue(e instanceof PulsarClientException);
        }

        mockBrokerService.resetHandleSubscribe();
    }

    @Test
    public void testSubscribeFailDoesNotFailOtherConsumer() throws Exception {
        subscribeFailDoesNotFailOtherConsumer("persistent://prop/use/ns/t1", "persistent://prop/use/ns/t2");
    }

    @Test
    public void testPartitionedSubscribeFailDoesNotFailOtherConsumer() throws Exception {
        subscribeFailDoesNotFailOtherConsumer("persistent://prop/use/ns/part-t1", "persistent://prop/use/ns/part-t2");
    }

    private void subscribeFailDoesNotFailOtherConsumer(String topic1, String topic2) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            if (counter.incrementAndGet() == 2) {
                // fail second producer
                ctx.writeAndFlush(Commands.newError(subscribe.getRequestId(), ServerError.AuthorizationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));

        });

        ConsumerBase<byte[]> consumer1 = (ConsumerBase<byte[]>) client.newConsumer().topic(topic1)
                .subscriptionName("sub1").subscribe();

        ConsumerBase<byte[]> consumer2 = null;
        try {
            consumer2 = (ConsumerBase<byte[]>) client.newConsumer().topic(topic2).subscriptionName("sub1").subscribe();
            fail("Should have failed");
        } catch (Exception e) {
            // ok
        }

        assertTrue(consumer1.isConnected());
        assertFalse(consumer2 != null && consumer2.isConnected());

        mockBrokerService.resetHandleSubscribe();
    }

    // failed to connect to partition at initialization step if a producer which connects to broker as lazy-loading mode
    @Test
    public void testPartitionedProducerFailOnInitialization() throws Throwable {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getHttpAddress()).build();
        final AtomicInteger producerCounter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (producerCounter.incrementAndGet() == 1) {
                ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.AuthorizationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer", SchemaVersion.Empty));
        });

        try {
            client.newProducer()
                    .enableLazyStartPartitionedProducers(true)
                    .accessMode(ProducerAccessMode.Shared)
                    .topic("persistent://prop/use/ns/multi-part-t1").create();
            fail("Should have failed with an authorization error");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.AuthorizationException);
        }
        assertEquals(producerCounter.get(), 1);

        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleCloseProducer();
    }

    // failed to connect to partition at sending step if a producer which connects to broker as lazy-loading mode
    @Test
    public void testPartitionedProducerFailOnSending() throws Throwable {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getHttpAddress()).build();
        final AtomicInteger producerCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        final String topicName = "persistent://prop/use/ns/multi-part-t1";

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (producerCounter.incrementAndGet() == 2) {
                ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.AuthorizationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer", SchemaVersion.Empty));
        });

        mockBrokerService.setHandleSend((ctx, send, headersAndPayload) ->
            ctx.writeAndFlush(Commands.newSendReceipt(send.getProducerId(), send.getSequenceId(), send.getHighestSequenceId(), 0L, 0L))
        );

        mockBrokerService.setHandleCloseProducer((ctx, closeProducer) -> {
            ctx.writeAndFlush(Commands.newSuccess(closeProducer.getRequestId()));
            closeCounter.incrementAndGet();
        });

        final PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>) client.newProducer()
                .enableLazyStartPartitionedProducers(true)
                .accessMode(ProducerAccessMode.Shared)
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        try {
            producer.send("msg".getBytes());
            fail("Should have failed with an not connected exception");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.NotConnectedException);
            assertEquals(producer.getProducers().size(), 1);
        }

        try {
            // recreate failed producer
            for (int i = 0; i < client.getPartitionsForTopic(topicName).get().size(); i++) {
                producer.send("msg".getBytes());
            }
            assertEquals(producer.getProducers().size(), client.getPartitionsForTopic(topicName).get().size());
            assertEquals(producerCounter.get(), 5);
        } catch (Exception e) {
            fail();
        }

        // should not call close
        assertEquals(closeCounter.get(), 0);

        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleCloseProducer();
    }

    // if a producer which doesn't connect as lazy-loading mode fails to connect while creating partitioned producer,
    // it should close all successful connections of other producers and fail
    @Test
    public void testOneProducerFailShouldCloseAllProducersInPartitionedProducer() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getHttpAddress()).build();
        final AtomicInteger producerCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (producerCounter.incrementAndGet() == 3) {
                ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.AuthorizationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer", SchemaVersion.Empty));
        });

        mockBrokerService.setHandleCloseProducer((ctx, closeProducer) -> {
            ctx.writeAndFlush(Commands.newSuccess(closeProducer.getRequestId()));
            closeCounter.incrementAndGet();
        });

        try {
            client.newProducer().topic("persistent://prop/use/ns/multi-part-t1").create();
            fail("Should have failed with an authorization error");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.AuthorizationException);
            // should call close for 3 partitions
            assertEquals(closeCounter.get(), 3);
        }

        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleCloseProducer();
    }

    // if a consumer fails to subscribe while creating partitioned consumer, it should close all successful connections
    // of other consumers and fail
    @Test
    public void testOneConsumerFailShouldCloseAllConsumersInPartitionedConsumer() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getHttpAddress()).build();
        final AtomicInteger subscribeCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            System.err.println("subscribeCounter: " + subscribeCounter.get());
            if (subscribeCounter.incrementAndGet() == 3) {
                ctx.writeAndFlush(Commands.newError(subscribe.getRequestId(), ServerError.AuthorizationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));
        });

        mockBrokerService.setHandleCloseConsumer((ctx, closeConsumer) -> {
            ctx.writeAndFlush(Commands.newSuccess(closeConsumer.getRequestId()));
            closeCounter.incrementAndGet();
        });

        try {
            client.newConsumer().topic("persistent://prop/use/ns/multi-part-t1").subscriptionName("sub1").subscribe();
            fail("Should have failed with an authorization error");
        } catch (PulsarClientException.AuthorizationException e) {
        }

        // should call close for 3 partitions
        assertEquals(closeCounter.get(), 3);

        mockBrokerService.resetHandleSubscribe();
        mockBrokerService.resetHandleCloseConsumer();
    }

    @Test
    public void testFlowSendWhenPartitionedSubscribeCompletes() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getHttpAddress()).build();

        AtomicInteger subscribed = new AtomicInteger();
        AtomicBoolean fail = new AtomicBoolean(false);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            subscribed.incrementAndGet();
            ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));

        });

        mockBrokerService.setHandleFlow((ctx, sendFlow) -> {
            if (subscribed.get() != 4) {
                fail.set(true);
            }
        });

        client.newConsumer().topic("persistent://prop/use/ns/multi-part-t1").subscriptionName("sub1").subscribe();

        if (fail.get()) {
            fail("Flow command should have been sent after all 4 partitions subscribe successfully");
        }

        mockBrokerService.resetHandleSubscribe();
        mockBrokerService.resetHandleFlow();
    }

    // Run this test multiple times to reproduce race conditions on reconnection logic
    @Test(invocationCount = 10, groups = "broker-api")
    public void testProducerReconnect() throws Exception {
        AtomicInteger numOfConnections = new AtomicInteger();
        AtomicReference<ChannelHandlerContext> channelCtx = new AtomicReference<>();
        AtomicBoolean msgSent = new AtomicBoolean();
        mockBrokerService.setHandleConnect((ctx, connect) -> {
            channelCtx.set(ctx);
            ctx.writeAndFlush(Commands.newConnected(connect.getProtocolVersion()));
            if (numOfConnections.incrementAndGet() == 2) {
                // close the cnx immediately when trying to connect the 2nd time
                ctx.channel().close();
            }
        });

        mockBrokerService.setHandleProducer((ctx, produce) -> {
            ctx.writeAndFlush(Commands.newProducerSuccess(produce.getRequestId(), "default-producer", SchemaVersion.Empty));
        });

        mockBrokerService.setHandleSend((ctx, sendCmd, headersAndPayload) -> {
            msgSent.set(true);
            ctx.writeAndFlush(Commands.newSendReceipt(0, 0, 0, 1, 1));
        });

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        Producer<byte[]> producer = client.newProducer().topic("persistent://prop/use/ns/t1").create();

        // close the cnx after creating the producer
        channelCtx.get().channel().close().get();

        producer.send(new byte[0]);

        assertTrue(msgSent.get());
        assertTrue(numOfConnections.get() >= 3);

        mockBrokerService.resetHandleConnect();
        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleSend();
    }

    @Test
    public void testConsumerReconnect() throws Exception {
        AtomicInteger numOfConnections = new AtomicInteger();
        AtomicReference<ChannelHandlerContext> channelCtx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        mockBrokerService.setHandleConnect((ctx, connect) -> {
            channelCtx.set(ctx);
            ctx.writeAndFlush(Commands.newConnected(connect.getProtocolVersion()));
            if (numOfConnections.incrementAndGet() == 2) {
                // close the cnx immediately when trying to connect the 2nd time
                ctx.channel().close();
            }
            if (numOfConnections.get() == 3) {
                latch.countDown();
            }
        });

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));
        });

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(mockBrokerService.getBrokerAddress()).build();
        client.newConsumer().topic("persistent://prop/use/ns/t1").subscriptionName("sub1").subscribe();

        // close the cnx after creating the producer
        channelCtx.get().channel().close();
        latch.await(5, TimeUnit.SECONDS);

        assertEquals(numOfConnections.get(), 3);

        mockBrokerService.resetHandleConnect();
        mockBrokerService.resetHandleSubscribe();
    }
}
