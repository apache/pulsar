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
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.BrokerMetadataException;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
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
        ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
        ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
        ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
        ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
    public void testSendReceiptForRemovedProducerIsIgnored() {
        ThreadFactory threadFactory = new DefaultThreadFactory("testSendReceiptForRemovedProducerIsIgnored");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
        cnx.state = ClientCnx.State.Ready;

        // A receipt for a producer the broker has already told the client to close, as happens when
        // its topic is terminated with sends in flight, is dropped rather than failing the connection.
        long producerId = 1;
        CommandSendReceipt receipt = new CommandSendReceipt()
                .setProducerId(producerId)
                .setSequenceId(5)
                .setHighestSequenceId(5);
        receipt.setMessageId().setLedgerId(3).setEntryId(7);
        cnx.handleSendReceipt(receipt);

        ProducerImpl<?> producer = mock(ProducerImpl.class);
        cnx.registerProducer(producerId, producer);
        cnx.handleSendReceipt(receipt);
        verify(producer).ackReceived(cnx, 5, 5, 3, 7);

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
    public void testScalableTopicSupportRequiresExplicitFeatureFlag() {
        // Older brokers either omit feature flags entirely or advertise only older flags.
        for (int mode = 0; mode < 4; mode++) {
            CommandConnected connected = new CommandConnected()
                    .setServerVersion("test")
                    .setProtocolVersion(21);
            if (mode == 1) {
                connected.setFeatureFlags().setSupportsTopicWatchers(true);
            } else if (mode >= 2) {
                connected.setFeatureFlags().setSupportsScalableTopics(mode == 3);
            }
            boolean expectedSupport = mode == 3;
            withConnection("testScalableTopicSupportRequiresExplicitFeatureFlag", cnx -> {
                cnx.handleConnected(connected);
                assertThat(cnx.isSupportsScalableTopics()).isEqualTo(expectedSupport);
            });
        }
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
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
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

    /**
     * Test that when the channel goes inactive before the handshake completes, the connection future
     * fails with ConnectException rather than a plain PulsarClientException, so that callers which
     * classify on ConnectException (e.g. retryable end-txn checks) see the specific type.
     */
    @Test
    public void testChannelInactiveFailsConnectionFutureWithConnectException() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testChannelInactiveFailsConnectionFutureWithConnectException"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setOperationTimeoutMs(10_000);
            conf.setKeepAliveIntervalSeconds(0);
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
            cnx.channelActive(ctx);

            // The CONNECT frame has been sent but no CONNECTED response received, so the future is pending
            assertFalse(cnx.connectionFuture().isDone());

            cnx.channelInactive(ctx);

            try {
                cnx.connectionFuture().get(2, TimeUnit.SECONDS);
                fail("Connection future should have failed");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.ConnectException);
            }
        } finally {
            eventLoop.shutdownGracefully().sync();
        }
    }

    /**
     * The request timeout task runs on the connection's own event loop. When a client shuts down, the other loops
     * of its group can terminate before this one is done with its connections, so neither scheduling the task nor
     * cancelling it may depend on them.
     */
    @Test
    public void testRequestTimeoutTaskRunsOnConnectionEventLoop() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testRequestTimeoutTaskRunsOnConnectionEventLoop"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setOperationTimeoutMs(10);
            conf.setKeepAliveIntervalSeconds(0);
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf,
                    terminatedEventLoopGroup("testRequestTimeoutTaskRunsOnConnectionEventLoop-other"));
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop.next());
            cnx.channelActive(ctx);

            assertThat(cnx.newLookup(null, 1L)).failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(PulsarClientException.TimeoutException.class);

            cnx.channelInactive(ctx);
        } finally {
            eventLoop.shutdownGracefully();
        }
    }

    /**
     * A lookup response sends the next waiting lookup and, for a TooManyRequests error, schedules the reset of the
     * connection's count of rejected requests. Both run on the connection's own event loop, so that the response
     * still completes its lookup, and the waiting one still gets sent, while the other loops of the group terminate.
     */
    @Test
    public void testLookupResponseHandOffsUseConnectionEventLoop() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testLookupResponseHandOffsUseConnectionEventLoop"));
        try {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setKeepAliveIntervalSeconds(0);
            conf.setConcurrentLookupRequest(1);
            conf.setMaxLookupRequest(2);
            ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf,
                    terminatedEventLoopGroup("testLookupResponseHandOffsUseConnectionEventLoop-other"));
            EventLoop connectionEventLoop = eventLoop.next();
            cnx.setCtx(mockChannelHandlerContext(connectionEventLoop));
            cnx.state = ClientCnx.State.Ready;

            CompletableFuture<BinaryProtoLookupService.LookupDataResult> first = cnx.newLookup(null, 1L);
            // Waits for the first lookup to complete
            CompletableFuture<BinaryProtoLookupService.LookupDataResult> second = cnx.newLookup(null, 2L);

            // Responses are handled on the connection's event loop, which runs the tasks they hand off in order
            connectionEventLoop.submit(() -> cnx.handleLookupResponse(new CommandLookupTopicResponse()
                    .setRequestId(1L)
                    .setResponse(CommandLookupTopicResponse.LookupType.Failed)
                    .setError(ServerError.TooManyRequests))).get(10, TimeUnit.SECONDS);
            assertThat(first).failsWithin(0, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(PulsarClientException.TooManyRequestsException.class);

            connectionEventLoop.submit(() -> cnx.handleLookupResponse(new CommandLookupTopicResponse()
                    .setRequestId(2L)
                    .setResponse(CommandLookupTopicResponse.LookupType.Connect)
                    .setBrokerServiceUrl("pulsar://broker:6650"))).get(10, TimeUnit.SECONDS);
            assertThat(second).isCompletedWithValueMatching(result -> "pulsar://broker:6650".equals(result.brokerUrl));
        } finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    public void testLookupAdmissionAndFifoHandoff() {
        EmbeddedChannel channel = newLookupChannel();
        ClientCnx cnx = channel.pipeline().get(ClientCnx.class);
        try {
            CompletableFuture<LookupDataResult> first = cnx.newLookup(Unpooled.copyInt(1), 1);
            CompletableFuture<LookupDataResult> second = cnx.newLookup(Unpooled.copyInt(2), 2);
            CompletableFuture<LookupDataResult> third = cnx.newLookup(Unpooled.copyInt(3), 3);
            ByteBuf rejected = Unpooled.copyInt(4);
            assertLookupFailure(cnx.newLookup(rejected, 4), PulsarClientException.TooManyRequestsException.class);
            assertThat(rejected.refCnt()).as("Rejected request buffer").isZero();
            assertOnlyLookupWritten(channel, 1);

            completeLookup(cnx, 1);
            assertThat(first).succeedsWithin(0, TimeUnit.SECONDS);
            channel.runPendingTasks();
            assertOnlyLookupWritten(channel, 2);
            completeLookup(cnx, 2);
            assertThat(second).succeedsWithin(0, TimeUnit.SECONDS);
            channel.runPendingTasks();
            assertOnlyLookupWritten(channel, 3);

            // Submit another lookup from the completion of the last active one.
            CompletableFuture<LookupDataResult> next = third.thenCompose(
                    ignored -> cnx.newLookup(Unpooled.copyInt(5), 5));
            completeLookup(cnx, 3);
            assertOnlyLookupWritten(channel, 5);
            completeLookup(cnx, 5);
            assertThat(next).succeedsWithin(0, TimeUnit.SECONDS);
            assertThat(cnx.getPendingLookupRequestSemaphore().availablePermits()).isOne();
            assertThat(cnx.idleCheck()).isTrue();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testDisconnectBeforeFastLookupRegistration() throws Exception {
        ClientCnx cnx = spy(newLookupClientCnx(mock(EventLoopGroup.class)));
        EmbeddedChannel channel = newLookupChannel(cnx);
        ChannelHandlerContext writes = useLookupExecutor(cnx, channel.eventLoop());
        CountDownLatch beforeRegistration = new CountDownLatch(1);
        CountDownLatch allowRegistration = new CountDownLatch(1);
        doAnswer(invocation -> {
            beforeRegistration.countDown();
            assertThat(allowRegistration.await(10, TimeUnit.SECONDS)).as("Disconnect finished before registration")
                    .isTrue();
            return invocation.callRealMethod();
        }).when(cnx).registerPendingLookup(eq(1L), any());

        ExecutorService submitter = Executors.newSingleThreadExecutor(new DefaultThreadFactory("lookup-submitter"));
        ByteBuf request = Unpooled.copyInt(1);
        try {
            Future<CompletableFuture<LookupDataResult>> submission = submitter.submit(() -> cnx.newLookup(request, 1));
            assertThat(beforeRegistration.await(10, TimeUnit.SECONDS)).as("Reached lookup registration").isTrue();
            assertThat(cnx.getPendingLookupRequestSemaphore().availablePermits()).isZero();
            assertThat(cnx.getPendingRequests().isEmpty()).isTrue();
            assertThat(cnx.idleCheck()).as("Lookup reserved a permit before publishing its future").isFalse();
            channel.close().get(10, TimeUnit.SECONDS);
            assertThat(request.refCnt()).as("Submitter still owns the unsent buffer").isOne();
            allowRegistration.countDown();

            assertLookupFailure(submission.get(10, TimeUnit.SECONDS), PulsarClientException.ConnectException.class);
            verify(writes, never()).writeAndFlush(any());
            assertThat(request.refCnt()).as("Unsent lookup buffer").isZero();
            assertThat(cnx.getPendingLookupRequestSemaphore().availablePermits()).isOne();
            assertThat(cnx.idleCheck()).isTrue();
        } finally {
            allowRegistration.countDown();
            submitter.shutdown();
            try {
                assertThat(submitter.awaitTermination(10, TimeUnit.SECONDS)).as("Lookup submitter terminated").isTrue();
            } finally {
                channel.finishAndReleaseAll();
                if (request.refCnt() > 0) {
                    request.release();
                }
            }
        }
    }

    @Test
    public void testClosingResponseFailsPromotedAndWaitingLookups() {
        EmbeddedChannel channel = newLookupChannel();
        ClientCnx cnx = channel.pipeline().get(ClientCnx.class);
        EventExecutor executor = mock(EventExecutor.class);
        ChannelHandlerContext writes = useLookupExecutor(cnx, executor);
        try {
            CompletableFuture<LookupDataResult> active = cnx.newLookup(Unpooled.copyInt(1), 1);
            ByteBuf promoted = Unpooled.copyInt(2);
            CompletableFuture<LookupDataResult> next = cnx.newLookup(promoted, 2);
            ByteBuf waiting = Unpooled.copyInt(3);
            CompletableFuture<LookupDataResult> queued = cnx.newLookup(waiting, 3);
            ByteBuf retry = Unpooled.copyInt(4);
            CompletableFuture<LookupDataResult> retried = queued.exceptionallyCompose(
                    error -> cnx.newLookup(retry, 4));
            assertOnlyLookupWritten(channel, 1);

            cnx.handleLookupResponse(new CommandLookupTopicResponse().setRequestId(1)
                    .setResponse(CommandLookupTopicResponse.LookupType.Failed).setError(ServerError.ServiceNotReady));
            channel.runPendingTasks();
            assertThat(channel.isActive()).isFalse();
            assertLookupFailure(active, PulsarClientException.ServiceNotReadyException.class);
            ByteBuf afterClose = Unpooled.copyInt(5);
            CompletableFuture<LookupDataResult> late = cnx.newLookup(afterClose, 5);
            for (CompletableFuture<?> future : List.of(next, queued, retried, late)) {
                assertLookupFailure(future, PulsarClientException.ConnectException.class);
            }
            // Disconnect already retired the promoted request when the event loop runs its deferred write.
            ArgumentCaptor<Runnable> handoff = ArgumentCaptor.forClass(Runnable.class);
            verify(executor).execute(handoff.capture());
            handoff.getValue().run();
            assertThat(promoted.refCnt()).as("Promoted buffer after its write task ran").isZero();
            assertThat(waiting.refCnt()).as("Queued buffer on disconnect").isZero();
            assertThat(retry.refCnt()).as("Buffer submitted during disconnect").isZero();
            assertThat(afterClose.refCnt()).as("Buffer submitted after disconnect").isZero();
            verify(writes).writeAndFlush(any());
            assertThat(cnx.getPendingLookupRequestSemaphore().availablePermits()).isOne();
            assertThat(cnx.idleCheck()).isTrue();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testRejectedLookupHandoffsReleaseTheirBuffersAndPermit() {
        EmbeddedChannel channel = newLookupChannel();
        ClientCnx cnx = channel.pipeline().get(ClientCnx.class);
        EventExecutor rejectingExecutor = mock(EventExecutor.class);
        doThrow(new RejectedExecutionException("Event loop stopped")).when(rejectingExecutor).execute(any());
        useLookupExecutor(cnx, rejectingExecutor);
        try {
            CompletableFuture<LookupDataResult> active = cnx.newLookup(Unpooled.copyInt(1), 1);
            ByteBuf second = Unpooled.copyInt(2);
            CompletableFuture<LookupDataResult> secondFuture = cnx.newLookup(second, 2);
            ByteBuf third = Unpooled.copyInt(3);
            CompletableFuture<LookupDataResult> thirdFuture = cnx.newLookup(third, 3);
            assertOnlyLookupWritten(channel, 1);
            completeLookup(cnx, 1);

            assertThat(active).succeedsWithin(0, TimeUnit.SECONDS);
            for (CompletableFuture<?> future : List.of(secondFuture, thirdFuture)) {
                assertLookupFailure(future, RejectedExecutionException.class);
            }
            assertThat(second.refCnt()).isZero();
            assertThat(third.refCnt()).isZero();
            assertThat(cnx.getPendingLookupRequestSemaphore().availablePermits()).isOne();
            CompletableFuture<LookupDataResult> fresh = cnx.newLookup(Unpooled.copyInt(4), 4);
            assertOnlyLookupWritten(channel, 4);
            completeLookup(cnx, 4);
            assertThat(fresh).succeedsWithin(0, TimeUnit.SECONDS);
            assertThat(cnx.idleCheck()).isTrue();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testConcurrentLookupSubmissionAndResponseMakeProgress() throws Exception {
        EventLoopGroup group = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("testConcurrentLookupSubmissionAndResponseMakeProgress"));
        try {
            EventLoop eventLoop = group.next();
            ClientCnx cnx = newLookupClientCnx(group);
            ChannelHandlerContext ctx = mockChannelHandlerContext(eventLoop);
            cnx.setCtx(ctx);
            cnx.state = ClientCnx.State.Ready;
            CyclicBarrier start = new CyclicBarrier(2);
            for (int round = 0; round < 16; round++) {
                int firstId = 2 * round;
                int nextId = firstId + 1;
                CompletableFuture<LookupDataResult> first = cnx.newLookup(Unpooled.copyInt(firstId), firstId);
                Future<?> response = eventLoop.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    completeLookup(cnx, firstId);
                    return null;
                });
                ByteBuf request = Unpooled.copyInt(nextId);
                start.await(10, TimeUnit.SECONDS);
                CompletableFuture<LookupDataResult> next = cnx.newLookup(request, nextId);
                response.get(10, TimeUnit.SECONDS);
                assertThat(first).succeedsWithin(0, TimeUnit.SECONDS);
                // Any handoff submitted by the response must precede this task on the connection's event loop.
                eventLoop.submit(() -> {
                    verify(ctx).writeAndFlush(same(request));
                    completeLookup(cnx, nextId);
                }).get(10, TimeUnit.SECONDS);
                assertThat(next).succeedsWithin(0, TimeUnit.SECONDS);
                assertThat(cnx.getPendingLookupRequestSemaphore().availablePermits()).isOne();
                assertThat(cnx.idleCheck()).isTrue();
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
        }
    }

    private static ChannelHandlerContext useLookupExecutor(ClientCnx cnx, EventExecutor executor) {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class, delegatesTo(cnx.ctx()));
        when(ctx.executor()).thenReturn(executor);
        cnx.setCtx(ctx);
        return ctx;
    }

    private static EmbeddedChannel newLookupChannel() {
        return newLookupChannel(newLookupClientCnx(mock(EventLoopGroup.class)));
    }

    private static EmbeddedChannel newLookupChannel(ClientCnx cnx) {
        EmbeddedChannel channel = new EmbeddedChannel(cnx);
        ByteBuf connect = channel.readOutbound();
        connect.release();
        cnx.handleConnected(new CommandConnected().setServerVersion("test")
                .setProtocolVersion(Commands.getCurrentProtocolVersion()));
        return channel;
    }

    private static ClientCnx newLookupClientCnx(EventLoopGroup eventLoopGroup) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setKeepAliveIntervalSeconds(0);
        conf.setConcurrentLookupRequest(1);
        conf.setMaxLookupRequest(3);
        return new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup);
    }

    private static void completeLookup(ClientCnx cnx, long requestId) {
        cnx.handleLookupResponse(new CommandLookupTopicResponse().setRequestId(requestId)
                .setResponse(CommandLookupTopicResponse.LookupType.Connect)
                .setBrokerServiceUrl("pulsar://broker:6650"));
    }

    private static void assertLookupFailure(CompletableFuture<?> future, Class<? extends Throwable> cause) {
        assertThat(future).failsWithin(0, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class).withCauseInstanceOf(cause);
    }

    private static void assertOnlyLookupWritten(EmbeddedChannel channel, int expectedId) {
        ByteBuf frame = channel.readOutbound();
        assertThat(frame).as("Outbound lookup %s", expectedId).isNotNull();
        try {
            assertThat(frame.readInt()).as("Lookup sent to the broker").isEqualTo(expectedId);
        } finally {
            frame.release();
        }
        assertThat(channel.outboundMessages()).as("Additional writes").isEmpty();
    }

    /**
     * Stands in for the other loops of a connection's event loop group, once they have terminated.
     */
    private static EventLoopGroup terminatedEventLoopGroup(String name) throws InterruptedException {
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory(name));
        eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
        return eventLoopGroup;
    }

    /**
     * A mocked context whose executor stands in for the connection's own event loop.
     */
    private static ChannelHandlerContext mockChannelHandlerContext(EventExecutor executor) {
        ChannelHandlerContext ctx = ClientTestFixtures.mockChannelHandlerContext();
        when(ctx.executor()).thenReturn(executor);
        return ctx;
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
