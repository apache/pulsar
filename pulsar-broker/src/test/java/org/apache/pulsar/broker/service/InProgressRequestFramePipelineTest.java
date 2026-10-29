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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InProgressRequestFramePipelineTest {
    @DataProvider
    public Object[][] proxyInputs() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "proxyInputs")
    public void shouldTrackFramesAfterProxyDetection(boolean proxyHeader, boolean fragmented) {
        InProgressRequestFrameMemoryController controller = controller();
        InProgressRequestFrameTrackingDecoder decoder =
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter());
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(OptionalProxyProtocolDecoder.NAME,
                new OptionalProxyProtocolDecoder(decoder::validateUpstreamHandlers));
        channel.pipeline().addLast(decoder);
        ByteBuf input = PooledByteBufAllocator.DEFAULT.directBuffer(256, 256);
        ByteBuf first = null;
        ByteBuf second = null;
        try {
            if (proxyHeader) {
                input.writeCharSequence("PROXY TCP4 127.0.0.1 127.0.0.1 12345 6650\r\n", StandardCharsets.US_ASCII);
            }
            input.writeInt(8).writeLong(1).writeInt(8).writeLong(2);
            if (fragmented) {
                // Independent transport reads, not slices of one allocation.
                ByteBuf prefix = Unpooled.directBuffer(6, 6).writeBytes(input, 6);
                assertThat(channel.writeInbound(prefix)).isFalse();
            }
            assertThat(channel.writeInbound(input)).isTrue();
            input = null;
            assertThat(channel.pipeline().get(OptionalProxyProtocolDecoder.class)).isNull();
            if (proxyHeader) {
                HAProxyMessage header = channel.readInbound();
                assertThat(header.sourcePort()).isEqualTo(12345);
                header.release();
            }
            first = channel.readInbound();
            second = channel.readInbound();
            assertThat(first.readLong()).isEqualTo(1);
            assertThat(second.readLong()).isEqualTo(2);
            long retained = controller.getRetainedFrameBytes();
            assertThat(retained).isPositive();
            first.release();
            first = null;
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(retained);
            channel.finishAndReleaseAll();
            second.release();
            second = null;
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            ReferenceCountUtil.safeRelease(input);
            ReferenceCountUtil.safeRelease(first);
            ReferenceCountUtil.safeRelease(second);
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldRejectUnknownHandlerRegardlessOfItsNameOnFirstRead() {
        InProgressRequestFrameMemoryController controller = controller();
        EmbeddedChannel channel = new EmbeddedChannel();
        // Adding the decoder to an already-active channel must still validate before it accounts for input.
        channel.pipeline().addLast("tls", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast(new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        ByteBuf input = Unpooled.directBuffer(12).writeInt(8).writeLong(1);
        try {
            assertThatThrownBy(() -> channel.writeInbound(input)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Unsupported upstream handler");
            assertThat(channel.isOpen()).isFalse();
            assertThat(input.refCnt()).isZero();
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldRejectSubclassAndInvalidOrderBeforeReading() {
        for (boolean subclass : new boolean[] {false, true}) {
            InProgressRequestFrameTrackingDecoder decoder =
                    new InProgressRequestFrameTrackingDecoder(1024, controller().getLimiter());
            EmbeddedChannel channel = new EmbeddedChannel();
            if (subclass) {
                channel.pipeline().addLast(new FlushConsolidationHandler() { });
            } else {
                channel.pipeline().addLast(new OptionalProxyProtocolDecoder());
                channel.pipeline().addLast(new FlushConsolidationHandler());
            }
            channel.pipeline().addLast(decoder);
            try {
                channel.pipeline().fireChannelActive();
                assertThatThrownBy(channel::checkException).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Unsupported upstream handler");
                assertThat(channel.isOpen()).isFalse();
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void shouldRevalidateProxyTransitionAndReleaseBufferedInputOnFailure() {
        InProgressRequestFrameMemoryController controller = controller();
        InProgressRequestFrameTrackingDecoder decoder =
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter());
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(OptionalProxyProtocolDecoder.NAME,
                new OptionalProxyProtocolDecoder(decoder::validateUpstreamHandlers));
        channel.pipeline().addLast("frameDecoder", decoder);
        channel.pipeline().fireChannelActive();
        ByteBuf prefix = Unpooled.directBuffer(6, 6).writeInt(8).writeZero(2);
        ByteBuf suffix = Unpooled.directBuffer(6, 6).writeZero(6);
        try {
            assertThat(channel.writeInbound(prefix)).isFalse();
            // The controlled PROXY transition must check the current pipeline, not the activation snapshot.
            channel.pipeline().addBefore("frameDecoder", "unexpected", new ChannelInboundHandlerAdapter());
            assertThatThrownBy(() -> channel.writeInbound(suffix)).isInstanceOf(IllegalStateException.class);
            assertThat(channel.isOpen()).isFalse();
            assertThat(prefix.refCnt()).isZero();
            assertThat(suffix.refCnt()).isZero();
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldTrackDecryptedFramesThroughTlsAndProxyDetection() throws Exception {
        File certificate = new File("src/test/resources/authentication/tls/hn-verification/broker-cert.pem");
        File key = new File("src/test/resources/authentication/tls/hn-verification/broker-key.pem");
        SslContext serverContext = SslContextBuilder.forServer(certificate, key).sslProvider(SslProvider.JDK).build();
        SslContext clientContext = SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        InProgressRequestFrameMemoryController controller = controller();
        InProgressRequestFrameTrackingDecoder decoder =
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter());
        SslHandler serverTls = serverContext.newHandler(PooledByteBufAllocator.DEFAULT);
        SslHandler clientTls = clientContext.newHandler(PooledByteBufAllocator.DEFAULT);
        EmbeddedChannel server = new EmbeddedChannel(new FlushConsolidationHandler(), serverTls,
                new ChannelOutboundHandlerAdapter(),
                new OptionalProxyProtocolDecoder(decoder::validateUpstreamHandlers), decoder);
        EmbeddedChannel client = new EmbeddedChannel(clientTls);
        ByteBuf first = null;
        ByteBuf second = null;
        try {
            for (int i = 0; i < 100 && (!serverTls.handshakeFuture().isDone()
                    || !clientTls.handshakeFuture().isDone()); i++) {
                transfer(client, server);
                transfer(server, client);
            }
            assertThat(serverTls.handshakeFuture().isSuccess()).isTrue();
            assertThat(clientTls.handshakeFuture().isSuccess()).isTrue();
            client.writeOutbound(Unpooled.directBuffer(24).writeInt(8).writeLong(1).writeInt(8).writeLong(2));
            transfer(client, server);
            first = server.readInbound();
            second = server.readInbound();
            assertThat(first.readLong()).isEqualTo(1);
            assertThat(second.readLong()).isEqualTo(2);
            long retained = controller.getRetainedFrameBytes();
            assertThat(retained).isGreaterThanOrEqualTo(24);
            first.release();
            first = null;
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(retained);
            second.release();
            second = null;
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            ReferenceCountUtil.safeRelease(first);
            ReferenceCountUtil.safeRelease(second);
            server.finishAndReleaseAll();
            client.finishAndReleaseAll();
        }
    }

    private static void transfer(EmbeddedChannel source, EmbeddedChannel target) {
        Object message;
        while ((message = source.readOutbound()) != null) {
            target.writeInbound(message);
        }
        source.runPendingTasks();
        target.runPendingTasks();
    }

    private static InProgressRequestFrameMemoryController controller() {
        return new InProgressRequestFrameMemoryController(InProgressRequestFrameMemoryLimitMode.TRACKING,
                64, () -> 0, Long.MAX_VALUE);
    }
}
