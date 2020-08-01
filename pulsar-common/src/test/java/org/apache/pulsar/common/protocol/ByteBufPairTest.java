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
package org.apache.pulsar.common.protocol;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
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

}
