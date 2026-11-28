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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.PulsarChannelInitializer.PulsarChannelOptions;
import org.apache.pulsar.broker.service.ServerCnxThrottleTracker.ThrottleType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarChannelInitializerTest {
    private final List<ByteBuf> frames = new ArrayList<>();
    private EventLoopGroup eventLoops;
    private Channel channel;
    private Socket client;
    private ServerCnxThrottleTracker throttleTracker;
    private ThrottleType throttleOnFirstFrame;
    private int receivedFrames;

    @BeforeMethod
    public void setup() throws Exception {
        receivedFrames = 0;
        throttleOnFirstFrame = ThrottleType.TopicPublishRate;
        ServiceConfiguration config = new ServiceConfiguration();
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getConfig()).thenReturn(config);
        ServerCnx serverCnx = mock(ServerCnx.class);
        when(serverCnx.getBrokerService()).thenReturn(mock(BrokerService.class));
        when(serverCnx.isActive()).thenReturn(true);
        CompletableFuture<ChannelHandlerContext> context = new CompletableFuture<>();
        when(serverCnx.ctx()).thenAnswer(invocation -> context.getNow(null));
        doAnswer(invocation -> {
            context.complete(invocation.getArgument(0));
            return null;
        }).when(serverCnx).handlerAdded(any());
        doAnswer(invocation -> {
            frames.add(invocation.getArgument(1));
            if (++receivedFrames == 1) {
                throttleTracker.markThrottled(throttleOnFirstFrame);
            }
            return null;
        }).when(serverCnx).channelRead(any(), any());
        throttleTracker = new ServerCnxThrottleTracker(serverCnx);

        PulsarChannelInitializer initializer = new PulsarChannelInitializer(pulsar,
                PulsarChannelOptions.builder().build()) {
            @Override
            protected ServerCnx newServerCnx(PulsarService service, String listenerName) {
                return serverCnx;
            }
        };
        eventLoops = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        Channel listener = new ServerBootstrap().group(eventLoops).channel(NioServerSocketChannel.class)
                .childHandler(initializer).bind("127.0.0.1", 0).sync().channel();
        client = new Socket();
        client.connect((InetSocketAddress) listener.localAddress(), 5000);
        ChannelHandlerContext ctx = context.get(5, TimeUnit.SECONDS);
        channel = ctx.channel();
        channel.eventLoop().submit(() -> {
            assertThat(channel.isActive()).isTrue();
            // The real ServerCnx enables reading only after the broker is ready.
            assertThat(channel.config().isAutoRead()).isFalse();
            channel.config().setAutoRead(true);
        }).get(5, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            try {
                if (eventLoops != null) {
                    // Termination closes the listening and accepted channels and finishes their callbacks before
                    // releasing the frames held by the mock business handler.
                    eventLoops.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
                }
            } finally {
                frames.forEach(ByteBuf::release);
                frames.clear();
                client = null;
                channel = null;
                eventLoops = null;
            }
        }
    }

    @DataProvider
    public Object[][] throttleTypes() {
        return Arrays.stream(ThrottleType.values()).map(type -> new Object[]{type}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "throttleTypes")
    public void testAlreadyReadFramesCompleteWhileThrottled(ThrottleType type) throws Exception {
        throttleOnFirstFrame = type;
        channel.eventLoop().submit(() -> {
            ByteBuf input = PooledByteBufAllocator.DEFAULT.directBuffer(64, 64);
            input.writeInt(4).writeInt(1).writeInt(4).writeInt(2);
            // Inject one known socket-read batch through the real broker pipeline. TCP writes need not map
            // one-to-one to reads, so sending two messages over the socket cannot reliably exercise this case.
            channel.pipeline().fireChannelRead(input);
            assertThat(channel.config().isAutoRead()).isFalse();
            assertThat(frames).as("all complete frames in the current read reach ServerCnx").hasSize(2);
            assertThat(frames.get(0).readInt()).isEqualTo(1);
            assertThat(frames.get(1).readInt()).isEqualTo(2);
            frames.removeFirst().release();
            assertThat(input.refCnt()).as("the second in-progress frame still retains the allocation").isPositive();
            frames.removeFirst().release();
            assertThat(input.refCnt()).as("no flow-control queue retains the allocation").isZero();
            throttleTracker.unmarkThrottled(type);
            assertThat(channel.config().isAutoRead()).isTrue();
        }).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPartialFrameCompletesAfterResume() throws Exception {
        channel.eventLoop().submit(() -> {
            ByteBuf input = PooledByteBufAllocator.DEFAULT.directBuffer(64, 64);
            input.writeInt(4).writeInt(1).writeInt(4).writeShort(0);
            channel.pipeline().fireChannelRead(input);
            assertThat(frames).hasSize(1);
            assertThat(channel.config().isAutoRead()).isFalse();
            frames.removeFirst().release();
            assertThat(input.refCnt()).as("the decoder retains the incomplete frame").isPositive();

            throttleTracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            ByteBuf remainder = PooledByteBufAllocator.DEFAULT.directBuffer(2, 2).writeShort(2);
            channel.pipeline().fireChannelRead(remainder);
            assertThat(frames).hasSize(1);
            assertThat(frames.getFirst().readInt()).isEqualTo(2);
            frames.removeFirst().release();
            assertThat(input.refCnt()).isZero();
            assertThat(remainder.refCnt()).isZero();
        }).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testCloseReleasesPartialFrame() throws Exception {
        ByteBuf input = channel.eventLoop().submit(() -> {
            ByteBuf partial = PooledByteBufAllocator.DEFAULT.directBuffer(64, 64);
            partial.writeInt(4).writeShort(0);
            channel.pipeline().fireChannelRead(partial);
            throttleTracker.markThrottled(ThrottleType.TopicPublishRate);
            assertThat(frames).isEmpty();
            assertThat(partial.refCnt()).isPositive();
            return partial;
        }).get(5, TimeUnit.SECONDS);
        channel.close().sync();
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(input.refCnt()).isZero());
    }

    @Test
    public void testSocketReadsWaitUntilAllThrottleReasonsClear() throws Exception {
        channel.eventLoop().submit(() -> {
            throttleTracker.markThrottled(ThrottleType.TopicPublishRate);
            throttleTracker.markThrottled(ThrottleType.ConnectionMaxPendingPublishRequestsExceeded);
            throttleTracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            assertThat(channel.config().isAutoRead()).isFalse();
        }).get(5, TimeUnit.SECONDS);
        client.getOutputStream().write(ByteBuffer.allocate(8).putInt(4).putInt(42).array());
        client.getOutputStream().flush();
        Awaitility.await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(5))
                .until(() -> channel.eventLoop().submit(() -> receivedFrames).get(5, TimeUnit.SECONDS) == 0);

        channel.eventLoop().submit(() -> throttleTracker.unmarkThrottled(
                ThrottleType.ConnectionMaxPendingPublishRequestsExceeded)).get(5, TimeUnit.SECONDS);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> channel.eventLoop().submit(() -> receivedFrames).get(5, TimeUnit.SECONDS) == 1);
        channel.eventLoop().submit(() -> {
            assertThat(frames.getFirst().readInt()).isEqualTo(42);
            frames.removeFirst().release();
        }).get(5, TimeUnit.SECONDS);
    }
}
