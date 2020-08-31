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
import java.nio.ByteBuffer;
import java.time.LocalDateTime;

public class LocalDateTimeSchemaTest {

    @Test
    public void testSchemaEncode() {
        LocalDateTimeSchema schema = LocalDateTimeSchema.of();
        LocalDateTime localDateTime = LocalDateTime.now();
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 2);
        byteBuffer.putLong(localDateTime.toLocalDate().toEpochDay());
        byteBuffer.putLong(localDateTime.toLocalTime().toNanoOfDay());
        byte[] expected = byteBuffer.array();
        Assert.assertEquals(expected, schema.encode(localDateTime));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        LocalDateTimeSchema schema = LocalDateTimeSchema.of();
        LocalDateTime localDateTime = LocalDateTime.now();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(Long.BYTES * 2);
        byte[] bytes = schema.encode(localDateTime);
        byteBuf.writeBytes(bytes);
        Assert.assertEquals(localDateTime, schema.decode(bytes));
        Assert.assertEquals(localDateTime, schema.decode(byteBuf));
    }

    @Test
    public void testSchemaDecode() {
        LocalDateTime localDateTime = LocalDateTime.of(2020, 8, 22, 2, 0, 0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 2);
        byteBuffer.putLong(localDateTime.toLocalDate().toEpochDay());
        byteBuffer.putLong(localDateTime.toLocalTime().toNanoOfDay());
        byte[] byteData = byteBuffer.array();
        long expectedEpochDay = localDateTime.toLocalDate().toEpochDay();
        long expectedNanoOfDay = localDateTime.toLocalTime().toNanoOfDay();

        LocalDateTimeSchema schema = LocalDateTimeSchema.of();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(Long.BYTES * 2);
        byteBuf.writeBytes(byteData);
        LocalDateTime decode = schema.decode(byteData);
        Assert.assertEquals(expectedEpochDay, decode.toLocalDate().toEpochDay());
        Assert.assertEquals(expectedNanoOfDay, decode.toLocalTime().toNanoOfDay());
        decode = schema.decode(byteBuf);
        Assert.assertEquals(expectedEpochDay, decode.toLocalDate().toEpochDay());
        Assert.assertEquals(expectedNanoOfDay, decode.toLocalTime().toNanoOfDay());
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
