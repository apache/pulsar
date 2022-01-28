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
import java.time.LocalDate;

public class LocalDateSchemaTest {

    @Test
    public void testSchemaEncode() {
        LocalDateSchema schema = LocalDateSchema.of();
        LocalDate localDate = LocalDate.now();
        byte[] expected = new byte[] {
                (byte) (localDate.toEpochDay() >>> 56),
                (byte) (localDate.toEpochDay() >>> 48),
                (byte) (localDate.toEpochDay() >>> 40),
                (byte) (localDate.toEpochDay() >>> 32),
                (byte) (localDate.toEpochDay() >>> 24),
                (byte) (localDate.toEpochDay() >>> 16),
                (byte) (localDate.toEpochDay() >>> 8),
                ((Long)localDate.toEpochDay()).byteValue()
        };
        Assert.assertEquals(expected, schema.encode(localDate));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        LocalDateSchema schema = LocalDateSchema.of();
        LocalDate localDate = LocalDate.now();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        byte[] bytes = schema.encode(localDate);
        byteBuf.writeBytes(bytes);
        Assert.assertEquals(localDate, schema.decode(bytes));
        Assert.assertEquals(localDate, schema.decode(byteBuf));
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

        LocalDateSchema schema = LocalDateSchema.of();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        byteBuf.writeBytes(byteData);
        Assert.assertEquals(expected, schema.decode(byteData).toEpochDay());
        Assert.assertEquals(expected, schema.decode(byteBuf).toEpochDay());
    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;

        Assert.assertNull(LocalDateSchema.of().encode(null));
        Assert.assertNull(LocalDateSchema.of().decode(byteBuf));
        Assert.assertNull(LocalDateSchema.of().decode(bytes));
    }

}
