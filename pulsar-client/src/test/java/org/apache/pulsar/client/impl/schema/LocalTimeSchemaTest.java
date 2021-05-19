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
import java.time.LocalTime;

public class LocalTimeSchemaTest {

    @Test
    public void testSchemaEncode() {
        LocalTimeSchema schema = LocalTimeSchema.of();
        LocalTime localTime = LocalTime.now();
        byte[] expected = new byte[] {
                (byte) (localTime.toNanoOfDay() >>> 56),
                (byte) (localTime.toNanoOfDay() >>> 48),
                (byte) (localTime.toNanoOfDay() >>> 40),
                (byte) (localTime.toNanoOfDay() >>> 32),
                (byte) (localTime.toNanoOfDay() >>> 24),
                (byte) (localTime.toNanoOfDay() >>> 16),
                (byte) (localTime.toNanoOfDay() >>> 8),
                ((Long)localTime.toNanoOfDay()).byteValue()
        };
        Assert.assertEquals(expected, schema.encode(localTime));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        LocalTimeSchema schema = LocalTimeSchema.of();
        LocalTime localTime = LocalTime.now();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        byte[] bytes = schema.encode(localTime);
        byteBuf.writeBytes(bytes);
        Assert.assertEquals(localTime, schema.decode(bytes));
        Assert.assertEquals(localTime, schema.decode(byteBuf));
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
       
        LocalTimeSchema schema = LocalTimeSchema.of();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        byteBuf.writeBytes(byteData);
        Assert.assertEquals(expected, schema.decode(byteData).toNanoOfDay());
        Assert.assertEquals(expected, schema.decode(byteBuf).toNanoOfDay());
    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;

        Assert.assertNull(LocalTimeSchema.of().encode(null));
        Assert.assertNull(LocalTimeSchema.of().decode(byteBuf));
        Assert.assertNull(LocalTimeSchema.of().decode(bytes));
    }

}
