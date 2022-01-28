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

public class DoubleSchemaTest {

    @Test
    public void testSchemaEncode() {
        DoubleSchema schema = DoubleSchema.of();
        double data = 12345678.1234;
        long longData = Double.doubleToLongBits(data);
        byte[] expected = new byte[] {
                (byte) (longData >>> 56),
                (byte) (longData >>> 48),
                (byte) (longData >>> 40),
                (byte) (longData >>> 32),
                (byte) (longData >>> 24),
                (byte) (longData >>> 16),
                (byte) (longData >>> 8),
                ((Long)longData).byteValue()
        };
        Assert.assertEquals(expected, schema.encode(data));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        DoubleSchema schema = DoubleSchema.of();
        Double dbl = 1234578.8754321;
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(8);
        byte[] bytes = schema.encode(dbl);
        byteBuf.writeBytes(bytes);
        Assert.assertEquals(dbl, schema.decode(bytes));
        Assert.assertEquals(dbl, schema.decode(byteBuf));
    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;
        Assert.assertNull(DoubleSchema.of().encode(null));
        Assert.assertNull(DoubleSchema.of().decode(byteBuf));
        Assert.assertNull(DoubleSchema.of().decode(bytes));
    }

}
