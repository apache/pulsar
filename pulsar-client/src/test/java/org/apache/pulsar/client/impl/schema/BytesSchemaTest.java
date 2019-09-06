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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.Test;

/**
 * Unit test {@link BytesSchema}.
 */
public class BytesSchemaTest {

    @Test
    public void testBytesSchemaOf() {
        testBytesSchema(BytesSchema.of());
    }

    @Test
    public void testSchemaBYTES() {
        testBytesSchema(Schema.BYTES);
    }

    private void testBytesSchema(Schema<byte[]> schema) {
        byte[] data = "hello world".getBytes(UTF_8);

        byte[] serializedData = schema.encode(data);
        assertSame(data, serializedData);

        byte[] deserializedData = schema.decode(serializedData);
        assertSame(data, deserializedData);
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(deserializedData.length);
        byteBuf.writeBytes(deserializedData);
        assertEquals(data, ((BytesSchema)schema).decode(byteBuf));

    }

}
