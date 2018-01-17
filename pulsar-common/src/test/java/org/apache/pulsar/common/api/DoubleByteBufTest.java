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
package org.apache.pulsar.common.api;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.common.api.DoubleByteBuf;
import org.testng.annotations.Test;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class DoubleByteBufTest {

    @Test
    public void testDoubleByteBuf() throws Exception {

        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        DoubleByteBuf buf = DoubleByteBuf.get(b1, b2);

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

}
