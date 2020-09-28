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
import java.time.Instant;

public class InstantSchemaTest {

    @Test
    public void testSchemaEncode() {
        InstantSchema schema = InstantSchema.of();
        Instant instant = Instant.now();
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        byteBuffer.putLong(instant.getEpochSecond());
        byteBuffer.putInt(instant.getNano());
        byte[] expected = byteBuffer.array();
        Assert.assertEquals(expected, schema.encode(instant));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        InstantSchema schema = InstantSchema.of();
        Instant instant = Instant.now();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(Long.BYTES + Integer.BYTES);
        byte[] bytes = schema.encode(instant);
        byteBuf.writeBytes(bytes);
        Assert.assertEquals(instant, schema.decode(bytes));
        Assert.assertEquals(instant, schema.decode(byteBuf));
    }

    @Test
    public void testSchemaDecode() {
        Instant instant = Instant.now();
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        byteBuffer.putLong(instant.getEpochSecond());
        byteBuffer.putInt(instant.getNano());
        byte[] byteData = byteBuffer.array();
        long epochSecond = instant.getEpochSecond();
        long nano = instant.getNano();

        InstantSchema schema = InstantSchema.of();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(Long.BYTES + Integer.BYTES);
        byteBuf.writeBytes(byteData);
        Instant decode = schema.decode(byteData);
        Assert.assertEquals(epochSecond, decode.getEpochSecond());
        Assert.assertEquals(nano, decode.getNano());
        decode = schema.decode(byteBuf);
        Assert.assertEquals(epochSecond, decode.getEpochSecond());
        Assert.assertEquals(nano, decode.getNano());
    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;

        Assert.assertNull(InstantSchema.of().encode(null));
        Assert.assertNull(InstantSchema.of().decode(byteBuf));
        Assert.assertNull(InstantSchema.of().decode(bytes));
    }

}
