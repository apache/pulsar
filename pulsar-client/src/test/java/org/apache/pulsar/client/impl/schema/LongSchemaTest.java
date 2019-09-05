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
package org.apache.pulsar.client.impl.schema;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LongSchemaTest {

    @Test
    public void testSchemaEncode() {
        LongSchema longSchema = LongSchema.of();
        Long data = 1234578l;
        byte[] expected = new byte[] {
                (byte) (data >>> 56),
                (byte) (data >>> 48),
                (byte) (data >>> 40),
                (byte) (data >>> 32),
                (byte) (data >>> 24),
                (byte) (data >>> 16),
                (byte) (data >>> 8),
                data.byteValue()
        };
        Assert.assertEquals(expected, longSchema.encode(data));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        LongSchema longSchema = LongSchema.of();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        long start = 348592040;
        for (int i = 0; i < 100; ++i) {
            byte[] encode = longSchema.encode(start + i);
            long decoded = longSchema.decode(encode);
            Assert.assertEquals(decoded, start + i);
            byteBuf.writerIndex(0);
            byteBuf.writeBytes(encode);

            decoded = longSchema.decode(byteBuf);
            Assert.assertEquals(decoded, start + i);
        }
    }

    @Test
    public void testSchemaDecode() {
        byte[] byteData = new byte[] {
               0,
               0,
               0,
               0,
               0,
               10,
               24,
               42
        };
        Long expected = 10*65536l + 24*256 + 42;
        LongSchema longSchema = LongSchema.of();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        byteBuf.writeBytes(byteData);

        Assert.assertEquals(expected, longSchema.decode(byteData));
        Assert.assertEquals(expected, longSchema.decode(byteBuf));
    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;
        Assert.assertNull(LongSchema.of().encode(null));
        Assert.assertNull(LongSchema.of().decode(byteBuf));
        Assert.assertNull(LongSchema.of().decode(bytes));
    }

}
