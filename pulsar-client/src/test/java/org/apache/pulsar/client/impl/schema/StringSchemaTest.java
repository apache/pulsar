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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test {@link StringSchema}.
 */
public class StringSchemaTest {

    @Test
    public void testUtf8Charset() {
        StringSchema schema = new StringSchema();
        SchemaInfo si = schema.getSchemaInfo();
        assertFalse(si.getProperties().containsKey(StringSchema.CHARSET_KEY));

        String myString = "my string for test";
        byte[] data = schema.encode(myString);
        assertArrayEquals(data, myString.getBytes(UTF_8));

        String decodedString = schema.decode(data);
        assertEquals(decodedString, myString);

        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(data.length);
        byteBuf.writeBytes(data);

        assertEquals(schema.decode(byteBuf), myString);
    }

    @Test
    public void testAsciiCharset() {
        StringSchema schema = new StringSchema(US_ASCII);
        SchemaInfo si = schema.getSchemaInfo();
        assertTrue(si.getProperties().containsKey(StringSchema.CHARSET_KEY));
        assertEquals(
            si.getProperties().get(StringSchema.CHARSET_KEY),
            US_ASCII.name()
        );

        String myString = "my string for test";
        byte[] data = schema.encode(myString);
        assertArrayEquals(data, myString.getBytes(US_ASCII));

        String decodedString = schema.decode(data);
        assertEquals(decodedString, myString);

        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(data.length);
        byteBuf.writeBytes(data);

        assertEquals(schema.decode(byteBuf), myString);
    }

    @Test
    public void testSchemaInfoWithoutCharset() {
        SchemaInfo si = new SchemaInfoImpl()
            .setName("test-schema-info-without-charset")
            .setType(SchemaType.STRING)
            .setSchema(new byte[0])
            .setProperties(Collections.emptyMap());
        StringSchema schema = StringSchema.fromSchemaInfo(si);

        String myString = "my string for test";
        byte[] data = schema.encode(myString);
        assertArrayEquals(data, myString.getBytes(UTF_8));

        String decodedString = schema.decode(data);
        assertEquals(decodedString, myString);

        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(data.length);
        byteBuf.writeBytes(data);
        assertEquals(schema.decode(byteBuf), myString);
    }

    @DataProvider(name = "charsets")
    public Object[][] charsets() {
        return new Object[][] {
            {
                UTF_8
            },
            {
                US_ASCII
            }
        };
    }

    @Test(dataProvider = "charsets")
    public void testSchemaInfoWithCharset(Charset charset) {
        Map<String, String> properties = new HashMap<>();
        properties.put(StringSchema.CHARSET_KEY, charset.name());
        SchemaInfo si = new SchemaInfoImpl()
            .setName("test-schema-info-without-charset")
            .setType(SchemaType.STRING)
            .setSchema(new byte[0])
            .setProperties(properties);
        StringSchema schema = StringSchema.fromSchemaInfo(si);

        String myString = "my string for test";
        byte[] data = schema.encode(myString);
        assertArrayEquals(data, myString.getBytes(charset));

        String decodedString = schema.decode(data);
        assertEquals(decodedString, myString);

        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(data.length);
        byteBuf.writeBytes(data);

        assertEquals(schema.decode(byteBuf), myString);
    }

}
