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

import java.util.Date;

public class DateSchemaTest {

    @Test
    public void testSchemaEncode() {
        DateSchema schema = DateSchema.of();
        Date data = new Date();
        byte[] expected = new byte[] {
                (byte) (data.getTime() >>> 56),
                (byte) (data.getTime() >>> 48),
                (byte) (data.getTime() >>> 40),
                (byte) (data.getTime() >>> 32),
                (byte) (data.getTime() >>> 24),
                (byte) (data.getTime() >>> 16),
                (byte) (data.getTime() >>> 8),
                ((Long)data.getTime()).byteValue()
        };
        Assert.assertEquals(expected, schema.encode(data));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        DateSchema schema = DateSchema.of();
        Date date = new Date();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        byte[] bytes = schema.encode(date);
        byteBuf.writeBytes(bytes);
        Assert.assertEquals(date, schema.decode(bytes));
        Assert.assertEquals(date, schema.decode(byteBuf));
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
        long expected = 10*65536 + 24*256 + 42;
        DateSchema schema = DateSchema.of();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        byteBuf.writeBytes(byteData);
        Assert.assertEquals(expected, schema.decode(byteData).getTime());
        Assert.assertEquals(expected, schema.decode(byteBuf).getTime());
    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;

        Assert.assertNull(DateSchema.of().encode(null));
        Assert.assertNull(DateSchema.of().decode(byteBuf));
        Assert.assertNull(DateSchema.of().decode(bytes));
    }

}
