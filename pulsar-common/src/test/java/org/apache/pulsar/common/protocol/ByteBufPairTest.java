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
package org.apache.pulsar.common.protocol;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.VoidChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class ByteBufPairTest {

    @Test
    public void testDoubleByteBuf() throws Exception {
        ByteBuf b1 = PulsarByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PulsarByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBufPair buf = ByteBufPair.get(b1, b2);

        assertEquals(buf.readableBytes(), 256);
        assertEquals(buf.getFirst(), b1);
        assertEquals(buf.getSecond(), b2);

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);
        assertEquals(b2.refCnt(), 1);

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEncoder() throws Exception {
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair buf = ByteBufPair.get(b1, b2);

        assertEquals(buf.readableBytes(), 10);
        assertEquals(buf.getFirst(), b1);
        assertEquals(buf.getSecond(), b2);

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);
        assertEquals(b2.refCnt(), 1);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.write(any(), any())).then(invocation -> {
            // Simulate a write on the context which releases the buffer
            ((ByteBuf) invocation.getArguments()[0]).release();
            return null;
        });

        ByteBufPair.ENCODER.write(ctx, buf, null);

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    @Test
    public void testCoalesce() {
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair buf = ByteBufPair.get(b1, b2);
        ByteBuf coalesced = ByteBufPair.coalesce(buf);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
        assertEquals(new String(ByteBufUtil.getBytes(coalesced)), "helloworld");
        coalesced.release();
    }

    /**
     * A frame must reach the outbound pipeline as a single message, not as a
     * header entry followed by a payload entry.
     */
    @Test
    public void testEncoderCommitsFrameAsSingleMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(ByteBufPair.ENCODER);
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair buf = ByteBufPair.get(b1, b2);

        channel.writeOutbound(buf);

        ByteBuf frame = channel.readOutbound();
        assertNotNull(frame);
        assertEquals(frame.readableBytes(), 10);
        assertEquals(new String(ByteBufUtil.getBytes(frame)), "helloworld");
        assertNull(channel.readOutbound());
        frame.release();
        channel.finishAndReleaseAll();
    }

    /** Same as above for the copying encoder used on the TLS path. */
    @Test
    public void testCopyingEncoderCommitsFrameAsSingleMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(ByteBufPair.COPYING_ENCODER);
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair buf = ByteBufPair.get(b1, b2);

        channel.writeOutbound(buf);

        ByteBuf frame = channel.readOutbound();
        assertNotNull(frame);
        assertEquals(new String(ByteBufUtil.getBytes(frame)), "helloworld");
        assertNull(channel.readOutbound());
        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
        frame.release();
        channel.finishAndReleaseAll();
    }

    /**
     * The same pair can be written multiple times, as a resend after reconnect does.
     */
    @Test
    public void testEncoderSupportsMultipleWritesOfSamePair() {
        EmbeddedChannel channelA = new EmbeddedChannel(ByteBufPair.ENCODER);
        EmbeddedChannel channelB = new EmbeddedChannel(ByteBufPair.ENCODER);
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair buf = ByteBufPair.get(b1, b2);

        // Each write takes its own pair claim first, as ProducerImpl does (op.cmd.retain()).
        buf.retain();
        channelA.writeOutbound(buf);
        buf.retain();
        channelB.writeOutbound(buf);

        ByteBuf frameA = channelA.readOutbound();
        ByteBuf frameB = channelB.readOutbound();
        assertEquals(new String(ByteBufUtil.getBytes(frameA)), "helloworld");
        assertEquals(new String(ByteBufUtil.getBytes(frameB)), "helloworld");
        frameA.release();
        frameB.release();

        assertEquals(buf.refCnt(), 1);
        buf.release();
        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
        channelA.finishAndReleaseAll();
        channelB.finishAndReleaseAll();
    }

    /**
     * If one pair's component is concurrently released (as the send-timeout disposal can do
     * during a reconnect window), every entry that still reaches the wire must be a complete
     * frame — a header-only entry would desynchronize every frame after it.
     */
    @Test
    public void testEncoderFailedBuildKeepsStreamAligned() {
        EmbeddedChannel channel = new EmbeddedChannel(ByteBufPair.ENCODER);
        ByteBufPair good = ByteBufPair.get(Unpooled.wrappedBuffer("good1".getBytes()),
                Unpooled.wrappedBuffer("good2".getBytes()));
        ByteBuf bad2 = Unpooled.wrappedBuffer("payload".getBytes());
        ByteBufPair bad = ByteBufPair.get(Unpooled.wrappedBuffer("head!".getBytes()), bad2);
        ByteBufPair good3 = ByteBufPair.get(Unpooled.wrappedBuffer("good3".getBytes()),
                Unpooled.wrappedBuffer("good4".getBytes()));
        bad2.release();

        for (ByteBufPair p : Arrays.asList(good, bad, good3)) {
            try {
                channel.writeOutbound(p);
            } catch (IllegalReferenceCountException acceptable) {
                // a failed write is acceptable here, a partial entry is not
            }
        }

        List<String> frames = new ArrayList<>();
        ByteBuf frame;
        while ((frame = channel.readOutbound()) != null) {
            frames.add(new String(ByteBufUtil.getBytes(frame)));
            frame.release();
        }
        assertEquals(frames, Arrays.asList("good1good2", "good3good4"));
        channel.finishAndReleaseAll();
    }

    @Test
    public void testEncoderFailedBuildCommitsNothing() {
        ByteBuf bad1 = Unpooled.wrappedBuffer("head!".getBytes());
        ByteBuf bad2 = Unpooled.wrappedBuffer("payload".getBytes());
        ByteBufPair bad = ByteBufPair.get(bad1, bad2);
        // Simulate the concurrent release of the second component before the encoder takes its claim.
        bad2.release();
        assertEquals(bad2.refCnt(), 0);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        DefaultChannelPromise promise = new DefaultChannelPromise(
                channelMock(), ImmediateEventExecutor.INSTANCE);

        bad.retain();
        ByteBufPair.ENCODER.write(ctx, bad, promise);

        verify(ctx, never()).write(any(), any());
        assertTrue(promise.isDone() && !promise.isSuccess(), "the write promise must be failed");
        // the composite returned the first component's claim; the finally block consumed the write claim
        assertEquals(bad1.refCnt(), 1);
        assertEquals(bad.refCnt(), 1);
    }

    @Test
    public void testEncoderAllocationFailureFailsWriteAndReleasesPair() {
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair buf = ByteBufPair.get(b1, b2);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        DefaultChannelPromise promise = new DefaultChannelPromise(
                channelMock(), ImmediateEventExecutor.INSTANCE);

        try (MockedStatic<Unpooled> pooled = mockStatic(Unpooled.class)) {
            pooled.when(() -> Unpooled.compositeBuffer(2)).thenThrow(new OutOfMemoryError("test"));
            ByteBufPair.ENCODER.write(ctx, buf, promise);
        }

        verify(ctx, never()).write(any(), any());
        assertTrue(promise.isDone() && !promise.isSuccess(), "the write promise must be failed");
        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    private static Channel channelMock() {
        Channel channel = mock(Channel.class);
        when(channel.eventLoop()).thenReturn(new EmbeddedChannel().eventLoop());
        when(channel.voidPromise()).thenReturn(new VoidChannelPromise(channel, true));
        return channel;
    }
}
