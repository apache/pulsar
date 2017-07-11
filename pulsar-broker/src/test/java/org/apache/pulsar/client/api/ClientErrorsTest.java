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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.PulsarClientException.LookupException;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.netty.channel.ChannelHandlerContext;

/**
 */
public class ClientErrorsTest {

    MockBrokerService mockBrokerService;
    private static final int WEB_SERVICE_PORT = PortManager.nextFreePort();
    private static final int WEB_SERVICE_TLS_PORT = PortManager.nextFreePort();
    private static final int BROKER_SERVICE_PORT = PortManager.nextFreePort();
    private static final int BROKER_SERVICE_TLS_PORT = PortManager.nextFreePort();
    private static int ASYNC_EVENT_COMPLETION_WAIT = 100;

    private final String ASSERTION_ERROR = "AssertionError";

    @BeforeClass
    public void setup() {
        mockBrokerService = new MockBrokerService(WEB_SERVICE_PORT, WEB_SERVICE_TLS_PORT, BROKER_SERVICE_PORT,
                BROKER_SERVICE_TLS_PORT);
        mockBrokerService.start();
    }

    @AfterClass
    public void teardown() {
        mockBrokerService.stop();
    }

    @Test
    public void testMockBrokerService() throws Exception {
        // test default actions of mock broker service
        try {
            PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);

            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Exclusive);
            Consumer consumer = client.subscribe("persistent://prop/use/ns/t1", "sub1", conf);

            Producer producer = client.createProducer("persistent://prop/use/ns/t1");
            Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
            producer.send("message".getBytes());
            Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

            consumer.unsubscribe();

            producer.close();
            consumer.close();
            client.close();
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
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
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
            Producer producer = client.createProducer(topic);
        } catch (Exception e) {
            if (e.getMessage().equals(ASSERTION_ERROR)) {
                fail("Producer create should not retry on auth error");
            }
            assertTrue(e instanceof PulsarClientException.AuthorizationException);
        }
        mockBrokerService.resetHandleProducer();
        client.close();
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
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (counter.incrementAndGet() == 2) {
                ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer"));
                return;
            }
            ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.ServiceNotReady, "msg"));
        });

        try {
            Producer producer = client.createProducer(topic);
        } catch (Exception e) {
            fail("Should not fail");
        }

        mockBrokerService.resetHandleProducer();
        client.close();
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
        ClientConfiguration conf = new ClientConfiguration();
        conf.setOperationTimeout(1, TimeUnit.SECONDS);

        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT, conf);
        final AtomicInteger counter = new AtomicInteger(0);

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

        try {
            Producer producer = client.createProducer(topic);
            fail("Should have failed");
        } catch (Exception e) {
            // we fail even on the retriable error
            assertTrue(e instanceof PulsarClientException.LookupException);
        }

        mockBrokerService.resetHandleProducer();
        client.close();
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
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (counter.incrementAndGet() == 2) {
                // fail second producer
                ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.AuthenticationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer"));

        });

        ProducerBase producer1 = (ProducerBase) client.createProducer(topic1);

        ProducerBase producer2 = null;
        try {
            producer2 = (ProducerBase) client.createProducer(topic2);
            fail("Should have failed");
        } catch (Exception e) {
            // ok
        }

        assertTrue(producer1.isConnected());
        assertFalse(producer2 != null && producer2.isConnected());

        mockBrokerService.resetHandleProducer();
        client.close();
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
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            int i = counter.incrementAndGet();
            if (i == 1 || i == 5) {
                // succeed on 1st and 5th attempts
                ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer"));
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
            ctx.writeAndFlush(Commands.newSendReceipt(0, 0, 1, 1));
        });

        try {
            Producer producer = client.createProducer(topic);
            producer.send("message".getBytes());
        } catch (Exception e) {
            fail("Should not fail");
        }

        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleSend();
        client.close();
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
        final String brokerUrl = "pulsar://127.0.0.1:" + BROKER_SERVICE_PORT;
        PulsarClient client = PulsarClient.create(brokerUrl);
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
            ctx.writeAndFlush(Commands.newLookupResponse(brokerUrl, null, true, LookupType.Connect,
                    lookup.getRequestId(), false));
        });

        try {
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Exclusive);
            Consumer consumer = client.subscribe(topic, "sub1", conf);
        } catch (Exception e) {
            if (e.getMessage().equals(ASSERTION_ERROR)) {
                fail("Subscribe should not retry on persistence error");
            }
            assertTrue(e instanceof PulsarClientException.BrokerPersistenceException);
        }
        mockBrokerService.resetHandlePartitionLookup();
        mockBrokerService.resetHandleLookup();
        client.close();

    }

    private void subscribeFailWithoutRetry(String topic) throws Exception {
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Exclusive);
            Consumer consumer = client.subscribe(topic, "sub1", conf);
        } catch (Exception e) {
            if (e.getMessage().equals(ASSERTION_ERROR)) {
                fail("Subscribe should not retry on persistence error");
            }
            assertTrue(e instanceof PulsarClientException.BrokerPersistenceException);
        }
        mockBrokerService.resetHandleSubscribe();
        client.close();
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
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            if (counter.incrementAndGet() == 2) {
                ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));
                return;
            }
            ctx.writeAndFlush(Commands.newError(subscribe.getRequestId(), ServerError.ServiceNotReady, "msg"));
        });

        try {
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Exclusive);
            Consumer consumer = client.subscribe(topic, "sub1", conf);
        } catch (Exception e) {
            fail("Should not fail");
        }

        mockBrokerService.resetHandleSubscribe();
        client.close();
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
        ClientConfiguration conf = new ClientConfiguration();
        conf.setOperationTimeout(200, TimeUnit.MILLISECONDS);

        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT, conf);
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
            ConsumerConfiguration cConf = new ConsumerConfiguration();
            cConf.setSubscriptionType(SubscriptionType.Exclusive);
            client.subscribe(topic, "sub1", cConf);
            fail("Should have failed");
        } catch (Exception e) {
            // we fail even on the retriable error
            assertEquals(e.getClass(), LookupException.class);
        }

        mockBrokerService.resetHandleSubscribe();
        client.close();
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
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        final AtomicInteger counter = new AtomicInteger(0);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            if (counter.incrementAndGet() == 2) {
                // fail second producer
                ctx.writeAndFlush(Commands.newError(subscribe.getRequestId(), ServerError.AuthenticationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));

        });

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        ConsumerBase consumer1 = (ConsumerBase) client.subscribe(topic1, "sub1", conf);

        ConsumerBase consumer2 = null;
        try {
            consumer2 = (ConsumerBase) client.subscribe(topic2, "sub1", conf);
            fail("Should have failed");
        } catch (Exception e) {
            // ok
        }

        assertTrue(consumer1.isConnected());
        assertFalse(consumer2 != null && consumer2.isConnected());

        mockBrokerService.resetHandleSubscribe();
        client.close();
    }

    // if a producer fails to connect while creating partitioned producer, it should close all successful connections of
    // other producers and fail
    @Test
    public void testOneProducerFailShouldCloseAllProducersInPartitionedProducer() throws Exception {
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        final AtomicInteger producerCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);

        mockBrokerService.setHandleProducer((ctx, producer) -> {
            if (producerCounter.incrementAndGet() == 3) {
                ctx.writeAndFlush(Commands.newError(producer.getRequestId(), ServerError.AuthorizationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer"));
        });

        mockBrokerService.setHandleCloseProducer((ctx, closeProducer) -> {
            ctx.writeAndFlush(Commands.newSuccess(closeProducer.getRequestId()));
            closeCounter.incrementAndGet();
        });

        try {
            Producer producer = client.createProducer("persistent://prop/use/ns/multi-part-t1");
            fail("Should have failed with an authorization error");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.AuthorizationException);
            // should call close for 3 partitions
            assertEquals(closeCounter.get(), 3);
        }

        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleCloseProducer();
        client.close();
    }

    // if a consumer fails to subscribe while creating partitioned consumer, it should close all successful connections
    // of other consumers and fail
    @Test
    public void testOneConsumerFailShouldCloseAllConsumersInPartitionedConsumer() throws Exception {
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        final AtomicInteger subscribeCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            if (subscribeCounter.incrementAndGet() == 3) {
                ctx.writeAndFlush(Commands.newError(subscribe.getRequestId(), ServerError.AuthenticationError, "msg"));
                return;
            }
            ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));

        });

        mockBrokerService.setHandleCloseConsumer((ctx, closeConsumer) -> {
            ctx.writeAndFlush(Commands.newSuccess(closeConsumer.getRequestId()));
            closeCounter.incrementAndGet();
        });

        try {
            Consumer consumer = client.subscribe("persistent://prop/use/ns/multi-part-t1", "my-sub");
            fail("Should have failed with an authentication error");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.AuthenticationException);
            // should call close for 3 partitions
            assertEquals(closeCounter.get(), 3);
        }

        mockBrokerService.resetHandleSubscribe();
        mockBrokerService.resetHandleCloseConsumer();
        client.close();
    }

    @Test
    public void testFlowSendWhenPartitionedSubscribeCompletes() throws Exception {
        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);

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

        Consumer consumer = client.subscribe("persistent://prop/use/ns/multi-part-t1", "my-sub");

        if (fail.get()) {
            fail("Flow command should have been sent after all 4 partitions subscribe successfully");
        }

        mockBrokerService.resetHandleSubscribe();
        mockBrokerService.resetHandleFlow();
        client.close();
    }

    // Run this test multiple times to reproduce race conditions on reconnection logic
    @Test(invocationCount = 10)
    public void testProducerReconnect() throws Exception {
        AtomicInteger numOfConnections = new AtomicInteger();
        AtomicReference<ChannelHandlerContext> channelCtx = new AtomicReference<>();
        AtomicBoolean msgSent = new AtomicBoolean();
        mockBrokerService.setHandleConnect((ctx, connect) -> {
            channelCtx.set(ctx);
            ctx.writeAndFlush(Commands.newConnected(connect.getProtocolVersion()));
            if (numOfConnections.incrementAndGet() == 2) {
                // close the cnx immediately when trying to conenct the 2nd time
                ctx.channel().close();
            }
        });

        mockBrokerService.setHandleProducer((ctx, produce) -> {
            ctx.writeAndFlush(Commands.newProducerSuccess(produce.getRequestId(), "default-producer"));
        });

        mockBrokerService.setHandleSend((ctx, sendCmd, headersAndPayload) -> {
            msgSent.set(true);
            ctx.writeAndFlush(Commands.newSendReceipt(0, 0, 1, 1));
        });

        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        Producer producer = client.createProducer("persistent://prop/use/ns/t1");

        // close the cnx after creating the producer
        channelCtx.get().channel().close().get();

        producer.send(new byte[0]);

        assertEquals(msgSent.get(), true);
        assertTrue(numOfConnections.get() >= 3);

        mockBrokerService.resetHandleConnect();
        mockBrokerService.resetHandleProducer();
        mockBrokerService.resetHandleSend();
        client.close();
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
                // close the cnx immediately when trying to conenct the 2nd time
                ctx.channel().close();
            }
            if (numOfConnections.get() == 3) {
                latch.countDown();
            }
        });

        mockBrokerService.setHandleSubscribe((ctx, subscribe) -> {
            ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));
        });

        PulsarClient client = PulsarClient.create("http://127.0.0.1:" + WEB_SERVICE_PORT);
        client.subscribe("persistent://prop/use/ns/t1", "sub");

        // close the cnx after creating the producer
        channelCtx.get().channel().close();
        latch.await(5, TimeUnit.SECONDS);

        assertEquals(numOfConnections.get(), 3);

        mockBrokerService.resetHandleConnect();
        mockBrokerService.resetHandleSubscribe();
        client.close();
    }
}
