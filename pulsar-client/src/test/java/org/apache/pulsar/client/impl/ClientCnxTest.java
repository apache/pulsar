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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.BrokerMetadataException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandError;
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
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);
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
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10_000);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);
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
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10_000);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);
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
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10_000);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);
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
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

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
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

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
    public void testHandleCloseConsumer() {
        ThreadFactory threadFactory = new DefaultThreadFactory("testHandleCloseConsumer");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

        long consumerId = 1;
        cnx.registerConsumer(consumerId, mock(ConsumerImpl.class));
        assertEquals(cnx.consumers.size(), 1);

        CommandCloseConsumer closeConsumer = new CommandCloseConsumer().setConsumerId(consumerId);
        cnx.handleCloseConsumer(closeConsumer);
        assertEquals(cnx.consumers.size(), 0);

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testHandleCloseProducer() {
        ThreadFactory threadFactory = new DefaultThreadFactory("testHandleCloseProducer");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

        long producerId = 1;
        cnx.registerProducer(producerId, mock(ProducerImpl.class));
        assertEquals(cnx.producers.size(), 1);

        CommandCloseProducer closeProducerCmd = new CommandCloseProducer().setProducerId(producerId);
        cnx.handleCloseProducer(closeProducerCmd);
        assertEquals(cnx.producers.size(), 0);

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testIdleCheckWithTopicListWatcher() {
        ClientCnx cnx =
                new ClientCnx(new ClientConfigurationData(), mock(EventLoopGroup.class));
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

    private void withConnection(String testName, Consumer<ClientCnx> test) {
        ThreadFactory threadFactory = new DefaultThreadFactory(testName);
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        try {

            ClientConfigurationData conf = new ClientConfigurationData();
            ClientCnx cnx = new ClientCnx(conf, eventLoop);

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
