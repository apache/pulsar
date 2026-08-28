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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.BrokerMetadataException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class ClientCnxTest {

    @Test
    public void testClientCnxTimeout() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
        ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
        cnx.channelActive(ctx);

        try {
            cnx.newLookup(null, 123).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
        }

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testPendingLookupRequestSemaphore() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10_000);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
        ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
        cnx.channelActive(ctx);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CompletableFuture<Exception> completableFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Thread.sleep(1_000);
                CompletableFuture<BinaryProtoLookupService.LookupDataResult> future =
                        cnx.newLookup(null, 123);
                countDownLatch.countDown();
                future.get();
            } catch (Exception e) {
                completableFuture.complete(e);
            }
        }).start();
        countDownLatch.await();
        cnx.channelInactive(ctx);
        assertTrue(completableFuture.get().getCause() instanceof PulsarClientException.ConnectException);
        // wait for subsequent calls over
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), conf.getConcurrentLookupRequest());
        });
        eventLoop.shutdownGracefully();
    }

    @Test
    public void testPendingLookupRequestSemaphoreServiceNotReady() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10_000);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
        ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
        cnx.channelActive(ctx);
        cnx.state = ClientCnx.State.Ready;
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CompletableFuture<Exception> completableFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Thread.sleep(1_000);
                CompletableFuture<BinaryProtoLookupService.LookupDataResult> future =
                        cnx.newLookup(null, 123);
                countDownLatch.countDown();
                future.get();
            } catch (Exception e) {
                completableFuture.complete(e);
            }
        }).start();
        countDownLatch.await();
        CommandError commandError = new CommandError();
        commandError.setRequestId(123L);
        commandError.setError(ServerError.ServiceNotReady);
        commandError.setMessage("Service not ready");
        cnx.handleError(commandError);
        assertTrue(completableFuture.get().getCause() instanceof PulsarClientException.LookupException);
        // wait for subsequent calls over
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), conf.getConcurrentLookupRequest());
        });
        eventLoop.shutdownGracefully();
    }

    @Test
    public void testPendingWaitingLookupRequestSemaphore() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10_000);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
        ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
        cnx.channelActive(ctx);
        for (int i = 0; i < 5001; i++) {
            cnx.newLookup(null, i);
        }
        cnx.channelInactive(ctx);
        // wait for subsequent calls over
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), conf.getConcurrentLookupRequest());
        });
        eventLoop.shutdownGracefully();
    }

    @Test
    public void testReceiveErrorAtSendConnectFrameState() throws Exception {
        ThreadFactory threadFactory = new DefaultThreadFactory("testReceiveErrorAtSendConnectFrameState");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);

        ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();

        Field ctxField = PulsarHandler.class.getDeclaredField("ctx");
        ctxField.setAccessible(true);
        ctxField.set(cnx, ctx);

        // set connection as SentConnectFrame
        Field cnxField = ClientCnx.class.getDeclaredField("state");
        cnxField.setAccessible(true);
        cnxField.set(cnx, ClientCnx.State.SentConnectFrame);

        // receive error
        CommandError commandError = new CommandError()
            .setRequestId(-1)
            .setError(ServerError.AuthenticationError)
            .setMessage("authentication was failed");
        try {
            cnx.handleError(commandError);
        } catch (Exception e) {
            fail("should not throw any error");
        }

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testGetLastMessageIdWithError() throws Exception {
        ThreadFactory threadFactory = new DefaultThreadFactory("testReceiveErrorAtSendConnectFrameState");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);

        ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();

        Field ctxField = PulsarHandler.class.getDeclaredField("ctx");
        ctxField.setAccessible(true);
        ctxField.set(cnx, ctx);

        final long requestId = 100;

        // set connection as SentConnectFrame
        Field cnxField = ClientCnx.class.getDeclaredField("state");
        cnxField.setAccessible(true);
        cnxField.set(cnx, ClientCnx.State.SentConnectFrame);

        ByteBuf getLastIdCmd = Commands.newGetLastMessageId(5, requestId);
        CompletableFuture<?> future = cnx.sendGetLastMessageId(getLastIdCmd, requestId);

        // receive error
        CommandError commandError = new CommandError()
            .setRequestId(requestId)
            .setError(ServerError.MetadataError)
            .setMessage("failed to read");
        cnx.handleError(commandError);

        try {
            future.get();
            fail("Should have failed");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), BrokerMetadataException.class);
        }

        eventLoop.shutdownGracefully();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testHandleCloseConsumer() {
        ThreadFactory threadFactory = new DefaultThreadFactory("testHandleCloseConsumer");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);

        long consumerId = 1;
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.getConfiguration()).thenReturn(conf);
        ConsumerImpl<?> consumer = mock(ConsumerImpl.class);
        when(consumer.getClient()).thenReturn(pulsarClient);
        cnx.registerConsumer(consumerId, consumer);
        assertEquals(cnx.consumers.size(), 1);

        CommandCloseConsumer closeConsumer = new CommandCloseConsumer().setConsumerId(consumerId).setRequestId(1);
        cnx.handleCloseConsumer(closeConsumer);
        assertEquals(cnx.consumers.size(), 0);

        verify(consumer).connectionClosed(cnx, Optional.empty(), Optional.empty());

        eventLoop.shutdownGracefully();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testHandleCloseProducer() {
        ThreadFactory threadFactory = new DefaultThreadFactory("testHandleCloseProducer");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);

        long producerId = 1;
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.getConfiguration()).thenReturn(conf);
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        when(producer.getClient()).thenReturn(pulsarClient);
        cnx.registerProducer(producerId, producer);
        assertEquals(cnx.producers.size(), 1);

        CommandCloseProducer closeProducerCmd = new CommandCloseProducer().setProducerId(producerId).setRequestId(1);
        cnx.handleCloseProducer(closeProducerCmd);
        assertEquals(cnx.producers.size(), 0);

        verify(producer).connectionClosed(cnx, Optional.empty(), Optional.empty());

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testIdleCheckWithTopicListWatcher() {
        ClientCnx cnx =
                new ClientCnx(InstrumentProvider.NOOP, new ClientConfigurationData(), mock(EventLoopGroup.class));
        // idle check should return true initially
        assertTrue(cnx.idleCheck());
        cnx.registerTopicListWatcher(0, mock(TopicListWatcher.class));
        // idle check should now return false since there's a registered watcher
        assertFalse(cnx.idleCheck());
    }

    @Test
    public void testNoWatchersWhenNoServerSupport() {
        withConnection("testNoWatchersWhenNoServerSupport", cnx -> {
            cnx.handleConnected(new CommandConnected()
                    .setServerVersion("Some old Server")
                    .setProtocolVersion(1));

            CompletableFuture<CommandWatchTopicListSuccess> result =
                    cnx.newWatchTopicList(Commands.newWatchTopicList(7, 5, "tenant/ns",
                            ".*", null), 7);
            assertTrue(result.isCompletedExceptionally());
            assertFalse(cnx.getTopicListWatchers().containsKey(5));
        });
    }

    @Test
    public void testCreateWatcher() {
        withConnection("testCreateWatcher", cnx -> {
            CommandConnected connected = new CommandConnected()
                    .setServerVersion("Some strange Server")
                    .setProtocolVersion(1);
            connected.setFeatureFlags().setSupportsTopicWatchers(true);
            cnx.handleConnected(connected);

            CompletableFuture<CommandWatchTopicListSuccess> result =
                    cnx.newWatchTopicList(Commands.newWatchTopicList(7, 5, "tenant/ns",
                            ".*", null), 7);
            verify(cnx.ctx()).writeAndFlush(any(ByteBuf.class));
            assertFalse(result.isDone());

            CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess()
                    .setRequestId(7)
                    .setWatcherId(5).setTopicsHash("f00");
            cnx.handleCommandWatchTopicListSuccess(success);
            assertThat(result.getNow(null))
                    .usingRecursiveComparison()
                    .comparingOnlyFields("requestId", "watcherId", "topicsHash");
        });
    }



    @Test
    public void testUpdateWatcher() {
        withConnection("testUpdateWatcher", cnx -> {
            CommandConnected connected = new CommandConnected()
                    .setServerVersion("Some Strange Server")
                    .setProtocolVersion(1);
            connected.setFeatureFlags().setSupportsTopicWatchers(true);
            cnx.handleConnected(connected);

            cnx.newWatchTopicList(Commands.newWatchTopicList(7, 5, "tenant/ns", ".*", null), 7);

            CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess()
                    .setRequestId(7)
                    .setWatcherId(5).setTopicsHash("f00");
            cnx.handleCommandWatchTopicListSuccess(success);

            TopicListWatcher watcher = mock(TopicListWatcher.class);
            cnx.registerTopicListWatcher(5, watcher);

            CommandWatchTopicUpdate update = new CommandWatchTopicUpdate()
                    .setWatcherId(5)
                    .setTopicsHash("ADD");
            update.addNewTopic("persistent://tenant/ns/topic");
            cnx.handleCommandWatchTopicUpdate(update);
            verify(watcher).handleCommandWatchTopicUpdate(update);
        });
    }

    /**
     * Test that when a lookup request times out, the semaphore is properly released
     * so that subsequent lookup requests can still be sent.
     */
    @Test
    public void testLookupTimeoutReleasesSemaphore() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testLookupTimeoutReleasesSemaphore"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setOperationTimeoutMs(10);
            conf.setKeepAliveIntervalSeconds(0);
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
            ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
            cnx.channelActive(ctx);

            int initialPermits = cnx.getPendingLookupRequestSemaphore().availablePermits();

            // Send a lookup request that will time out
            CompletableFuture<BinaryProtoLookupService.LookupDataResult> future =
                    cnx.newLookup(null, 1L);

            // Wait for the timeout to trigger
            try {
                future.get(2, TimeUnit.SECONDS);
                fail("Should have timed out");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
            }

            // Verify semaphore is released back to initial permits
            Awaitility.await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), initialPermits);
            });
        } finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    /**
     * Test that when a lookup request times out, waiting lookup requests in the queue
     * are properly dispatched (the waiting queue is driven).
     */
    @Test
    public void testLookupTimeoutDrivesWaitingQueue() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testLookupTimeoutDrivesWaitingQueue"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            // concurrentLookupRequest=50 by default, maxLookupRequest=50000 by default
            conf.setOperationTimeoutMs(50);
            conf.setKeepAliveIntervalSeconds(0);
            conf.setConcurrentLookupRequest(1); // Only allow 1 concurrent lookup
            conf.setMaxLookupRequest(10);       // Allow up to 10 total (9 waiting)
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
            ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
            cnx.channelActive(ctx);
            cnx.state = ClientCnx.State.Ready;

            // First lookup occupies the only concurrent slot
            CompletableFuture<BinaryProtoLookupService.LookupDataResult> firstFuture =
                    cnx.newLookup(null, 1L);

            // Second lookup should go into the waiting queue
            CompletableFuture<BinaryProtoLookupService.LookupDataResult> secondFuture =
                    cnx.newLookup(null, 2L);

            // The second future should not be completed yet (it's waiting)
            assertFalse(secondFuture.isDone());

            // Wait for the first request to time out - this should drive the waiting queue
            // and dispatch the second request
            try {
                firstFuture.get(2, TimeUnit.SECONDS);
                fail("First future should have timed out");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
            }

            // After the first request times out, the second request should have been
            // dispatched from the waiting queue (it will also eventually time out)
            try {
                secondFuture.get(2, TimeUnit.SECONDS);
                fail("Second future should have timed out");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
            }

            // Verify all semaphores are properly released
            Awaitility.await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), 1);
            });
        } finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    /**
     * Test that when multiple lookup requests time out, all waiting requests in the queue
     * are eventually dispatched and no semaphore leak occurs.
     */
    @Test
    public void testMultipleLookupTimeoutsNoSemaphoreLeak() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testMultipleLookupTimeoutsNoSemaphoreLeak"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setOperationTimeoutMs(30);
            conf.setKeepAliveIntervalSeconds(0);
            conf.setConcurrentLookupRequest(2);  // Allow 2 concurrent lookups
            conf.setMaxLookupRequest(10);        // Allow up to 10 total (8 waiting)
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
            ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
            cnx.channelActive(ctx);
            cnx.state = ClientCnx.State.Ready;

            // Send 5 lookup requests: 2 will be concurrent, 3 will be in waiting queue
            CompletableFuture<?>[] futures = new CompletableFuture[5];
            for (int i = 0; i < 5; i++) {
                futures[i] = cnx.newLookup(null, i + 1L);
            }

            // Wait for all futures to complete (all should time out eventually)
            for (int i = 0; i < 5; i++) {
                try {
                    futures[i].get(5, TimeUnit.SECONDS);
                    fail("Future " + i + " should have timed out");
                } catch (Exception e) {
                    assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException,
                            "Future " + i + " should fail with TimeoutException but got: " + e.getCause());
                }
            }

            // Verify all semaphore permits are released (no leak)
            Awaitility.await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), 2);
            });
        } finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    /**
     * Test that when a failed lookup response is received,
     * the semaphore permit is properly released via the unified cleanup primitive
     * (removePendingRequest) without double-release.
     */
    @Test
    public void testFailedLookupResponseReleasesSemaphore() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testFailedLookupResponseReleasesSemaphore"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setOperationTimeoutMs(10_000);
            conf.setKeepAliveIntervalSeconds(0);
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
            ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
            cnx.channelActive(ctx);
            cnx.state = ClientCnx.State.Ready;

            int initialPermits = cnx.getPendingLookupRequestSemaphore().availablePermits();

            // Send a lookup request
            CompletableFuture<BinaryProtoLookupService.LookupDataResult> future =
                    cnx.newLookup(null, 1L);

            // Simulate a failed lookup response (CommandLookupTopicResponse with Failed status)
            CommandLookupTopicResponse lookupResponse = new CommandLookupTopicResponse();
            lookupResponse.setRequestId(1L);
            lookupResponse.setResponse(CommandLookupTopicResponse.LookupType.Failed);
            lookupResponse.setError(ServerError.ServiceNotReady);
            lookupResponse.setMessage("Service not ready");
            cnx.handleLookupResponse(lookupResponse);

            // Verify the future completed exceptionally
            try {
                future.get(2, TimeUnit.SECONDS);
                fail("Should have failed");
            } catch (Exception e) {
                // expected
            }

            // Verify semaphore is released back to initial permits (no leak, no double-release)
            // Previously this would cause availablePermits to be initialPermits+1 due to double-release
            Awaitility.await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), initialPermits);
            });
        } finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    /**
     * Test that when the connection is closed while an active lookup request is pending,
     * the semaphore permit is properly released.
     */
    @Test
    public void testActiveRequestConnectionCloseReleasesSemaphore() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testActiveRequestConnectionCloseReleasesSemaphore"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setOperationTimeoutMs(10_000);
            conf.setKeepAliveIntervalSeconds(0);
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
            ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
            cnx.channelActive(ctx);

            int initialPermits = cnx.getPendingLookupRequestSemaphore().availablePermits();

            // Send a lookup request
            CompletableFuture<BinaryProtoLookupService.LookupDataResult> future =
                    cnx.newLookup(null, 1L);

            // Simulate connection close
            cnx.channelInactive(ctx);

            // Verify the future completed exceptionally with ConnectException
            try {
                future.get(2, TimeUnit.SECONDS);
                fail("Should have failed");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof PulsarClientException.ConnectException);
            }

            // Verify semaphore is released back to initial permits
            Awaitility.await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), initialPermits);
            });
        } finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    /**
     * Test that when the connection is closed while a promoted waiting request is active,
     * the semaphore permit is properly released and no leak occurs.
     */
    @Test
    public void testPromotedWaitingRequestConnectionCloseReleasesSemaphore() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testPromotedWaitingRequestConnectionCloseReleasesSemaphore"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setOperationTimeoutMs(10_000);
            conf.setKeepAliveIntervalSeconds(0);
            conf.setConcurrentLookupRequest(1); // Only allow 1 concurrent lookup
            conf.setMaxLookupRequest(10);
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
            ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
            cnx.channelActive(ctx);
            cnx.state = ClientCnx.State.Ready;

            // First lookup occupies the only concurrent slot
            CompletableFuture<BinaryProtoLookupService.LookupDataResult> firstFuture =
                    cnx.newLookup(null, 1L);

            // Second lookup goes into the waiting queue
            CompletableFuture<BinaryProtoLookupService.LookupDataResult> secondFuture =
                    cnx.newLookup(null, 2L);

            // Simulate a failed lookup response for the first request, which promotes the second
            CommandLookupTopicResponse failedResponse = new CommandLookupTopicResponse();
            failedResponse.setRequestId(1L);
            failedResponse.setResponse(CommandLookupTopicResponse.LookupType.Failed);
            failedResponse.setError(ServerError.ServiceNotReady);
            failedResponse.setMessage("Service not ready");
            cnx.handleLookupResponse(failedResponse);

            // Verify the first future completed exceptionally
            assertTrue(firstFuture.isCompletedExceptionally());

            // Wait for the second request to be promoted from the waiting queue
            Awaitility.await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> cnx.pendingRequests.containsKey(2L));

            // Now close the connection while the promoted request is active
            cnx.channelInactive(ctx);

            // Verify the second future completed exceptionally
            try {
                secondFuture.get(2, TimeUnit.SECONDS);
                fail("Second future should have failed");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof PulsarClientException.ConnectException);
            }

            // Verify semaphore is released (1 permit for concurrentLookupRequest=1)
            Awaitility.await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(cnx.getPendingLookupRequestSemaphore().availablePermits(), 1);
            });
        } finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    private void withConnection(String testName, Consumer<ClientCnx> test) {
        ThreadFactory threadFactory = new DefaultThreadFactory(testName);
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        try {

            ClientConfigurationData conf = new ClientConfigurationData();
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);

            ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();

            Field ctxField = PulsarHandler.class.getDeclaredField("ctx");
            ctxField.setAccessible(true);
            ctxField.set(cnx, ctx);

            // set connection as SentConnectFrame
            Field cnxField = ClientCnx.class.getDeclaredField("state");
            cnxField.setAccessible(true);
            cnxField.set(cnx, ClientCnx.State.SentConnectFrame);

            test.accept(cnx);

        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Error using reflection on ClientCnx", e);
        } finally {
            eventLoop.shutdownGracefully();
        }
    }

}
