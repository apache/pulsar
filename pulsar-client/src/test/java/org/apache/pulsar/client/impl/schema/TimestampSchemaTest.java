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

import java.sql.Timestamp;

public class TimestampSchemaTest {

    @Test
    public void testSchemaEncode() {
        TimestampSchema schema = TimestampSchema.of();
        Timestamp data = new Timestamp(System.currentTimeMillis());
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
        TimestampSchema schema = TimestampSchema.of();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Assert.assertEquals(timestamp, schema.decode(schema.encode(timestamp)));
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

        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(byteData.length);
        byteBuf.writeBytes(byteData);
        long expected = 10*65536 + 24*256 + 42;
        TimestampSchema schema = TimestampSchema.of();
        Assert.assertEquals(expected, schema.decode(byteData).getTime());
        Assert.assertEquals(expected, schema.decode(byteBuf).getTime());

    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;
        Assert.assertNull(TimestampSchema.of().encode(null));
        Assert.assertNull(TimestampSchema.of().decode(byteBuf));
        Assert.assertNull(TimestampSchema.of().decode(bytes));
    }

}
