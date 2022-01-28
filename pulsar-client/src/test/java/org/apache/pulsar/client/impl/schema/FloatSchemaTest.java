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

public class FloatSchemaTest {

    @Test
    public void testSchemaEncode() {
        FloatSchema schema = FloatSchema.of();
        float data = (float) 12345678.1234;
        long longData = Float.floatToRawIntBits(data);
        byte[] expected = new byte[] {
                (byte) (longData >>> 24),
                (byte) (longData >>> 16),
                (byte) (longData >>> 8),
                ((Long)longData).byteValue()
        };
        Assert.assertEquals(expected, schema.encode(data));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        FloatSchema schema = FloatSchema.of();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(4);
        Float dbl = (float) 1234578.8754321;
        byte[] bytes = schema.encode(dbl);
        byteBuf.writeBytes(schema.encode(dbl));
        Assert.assertEquals(dbl, schema.decode(bytes));
        Assert.assertEquals(dbl, schema.decode(byteBuf));

    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;
        Assert.assertNull(FloatSchema.of().encode(null));
        Assert.assertNull(FloatSchema.of().decode(bytes));
        Assert.assertNull(FloatSchema.of().decode(byteBuf));
    }
}


