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

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.apache.pulsar.client.impl.MessagePayloadImpl;
import org.apache.pulsar.client.impl.MessagePayloadUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test of {@link MessagePayload}.
 */
public class MessagePayloadTest {

    @Test
    public void testConvertMessagePayloadImpl() {
        final ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(1);

        final MessagePayloadImpl payload = MessagePayloadImpl.create(buf);
        Assert.assertEquals(buf.refCnt(), 1);

        final ByteBuf convertedBuf = MessagePayloadUtils.convertToByteBuf(payload);
        Assert.assertSame(convertedBuf, buf);

        Assert.assertEquals(buf.refCnt(), 2);
        buf.release();
        buf.release();
    }

    @Test
    public void testConvertCustomPayload() {
        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(new byte[]{ 0x11, 0x22, 0x33 });
        buffer.flip();
        buffer.get(); // skip 1st byte

        final ByteBuf buf = MessagePayloadUtils.convertToByteBuf(new ByteBufferPayload(buffer));
        Assert.assertEquals(buf.refCnt(), 1);

        Assert.assertEquals(buf.readableBytes(), 2);
        Assert.assertEquals(buf.readByte(), 0x22);
        Assert.assertEquals(buf.readByte(), 0x33);

        buf.release();
    }

    @Test
    public void testConvertEmptyCustomPayload() {
        final ByteBuf buf = MessagePayloadUtils.convertToByteBuf(new ByteBufferPayload(ByteBuffer.allocate(0)));
        Assert.assertEquals(buf.refCnt(), 1);
        Assert.assertEquals(buf.readableBytes(), 0);
        buf.release();
    }

    private static class ByteBufferPayload implements MessagePayload {

        private final ByteBuffer buffer;

        public ByteBufferPayload(final ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public byte[] copiedBuffer() {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
    }

    @Test
    public void testFactoryWrap() {
        MessagePayloadImpl payload = (MessagePayloadImpl) MessagePayloadFactory.DEFAULT.wrap(new byte[1]);
        ByteBuf byteBuf = payload.getByteBuf();
        Assert.assertEquals(byteBuf.refCnt(), 1);
        payload.release();
        Assert.assertEquals(byteBuf.refCnt(), 0);

        payload = (MessagePayloadImpl) MessagePayloadFactory.DEFAULT.wrap(ByteBuffer.allocate(1));
        byteBuf = payload.getByteBuf();
        Assert.assertEquals(byteBuf.refCnt(), 1);
        payload.release();
        Assert.assertEquals(byteBuf.refCnt(), 0);
    }
}
