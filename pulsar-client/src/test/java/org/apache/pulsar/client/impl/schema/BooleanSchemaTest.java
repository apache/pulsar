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

public class BooleanSchemaTest {

    @Test
    public void testSchemaEncode() {
        BooleanSchema schema = BooleanSchema.of();
        byte[] expectedTrue = new byte[] {
                1
        };
        byte[] expectedFalse = new byte[] {
                0
        };
        Assert.assertEquals(expectedTrue, schema.encode(true));
        Assert.assertEquals(expectedFalse, schema.encode(false));
    }

    @Test
    public void testSchemaEncodeDecodeFidelity() {
        BooleanSchema schema = BooleanSchema.of();
        Assert.assertEquals(new Boolean(true), schema.decode(schema.encode(true)));
        Assert.assertEquals(new Boolean(false), schema.decode(schema.encode(false)));
    }

    @Test
    public void testSchemaDecode() {
        byte[] trueBytes = new byte[] {
                1
        };
        byte[] falseBytes = new byte[] {
                0
        };
        BooleanSchema schema = BooleanSchema.of();
        Assert.assertEquals(new Boolean(true), schema.decode(trueBytes));
        Assert.assertEquals(new Boolean(false), schema.decode(falseBytes));

        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(1);
        byteBuf.writeBytes(trueBytes);
        Assert.assertEquals(new Boolean(true), schema.decode(byteBuf));
        byteBuf.writerIndex(0);
        byteBuf.writeBytes(falseBytes);

        Assert.assertEquals(new Boolean(false), schema.decode(byteBuf));
    }

    @Test
    public void testNullEncodeDecode() {
        ByteBuf byteBuf = null;
        byte[] bytes = null;
        Assert.assertNull(BooleanSchema.of().encode(null));
        Assert.assertNull(BooleanSchema.of().decode(byteBuf));
        Assert.assertNull(BooleanSchema.of().decode(bytes));
    }

}
