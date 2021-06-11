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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Schema definition for Strings encoded in UTF-8 format.
 */
public class StringSchema extends AbstractSchema<String> {

    static final String CHARSET_KEY;

    private static final SchemaInfo DEFAULT_SCHEMA_INFO;
    private static final Charset DEFAULT_CHARSET;
    private static final StringSchema UTF8;

    static {
        // Ensure the ordering of the static initialization
        CHARSET_KEY = "__charset";
        DEFAULT_CHARSET = StandardCharsets.UTF_8;
        DEFAULT_SCHEMA_INFO = new SchemaInfoImpl()
                .setName("String")
                .setType(SchemaType.STRING)
                .setSchema(new byte[0]);

        UTF8 = new StringSchema(StandardCharsets.UTF_8);
    }

    private static final FastThreadLocal<byte[]> tmpBuffer = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[1024];
        }
    };

    public static StringSchema fromSchemaInfo(SchemaInfo schemaInfo) {
        checkArgument(SchemaType.STRING == schemaInfo.getType(), "Not a string schema");
        String charsetName = schemaInfo.getProperties().get(CHARSET_KEY);
        if (null == charsetName) {
            return UTF8;
        } else {
            return new StringSchema(Charset.forName(charsetName));
        }
    }

    public static StringSchema utf8() {
        return UTF8;
    }

    private final Charset charset;
    private final SchemaInfo schemaInfo;

    public StringSchema() {
        this.charset = DEFAULT_CHARSET;
        this.schemaInfo = DEFAULT_SCHEMA_INFO;
    }

    public StringSchema(Charset charset) {
        this.charset = charset;
        Map<String, String> properties = new HashMap<>();
        properties.put(CHARSET_KEY, charset.name());
        this.schemaInfo = new SchemaInfoImpl()
                .setName(DEFAULT_SCHEMA_INFO.getName())
                .setType(SchemaType.STRING)
                .setSchema(DEFAULT_SCHEMA_INFO.getSchema())
                .setProperties(properties);
    }

    public byte[] encode(String message) {
        if (null == message) {
            return null;
        } else {
            return message.getBytes(charset);
        }
    }

    public String decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        } else {
            return new String(bytes, charset);
        }
    }

    public String decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        } else {
            int size = byteBuf.readableBytes();
            byte[] bytes = tmpBuffer.get();
            if (size > bytes.length) {
                bytes = new byte[size * 2];
                tmpBuffer.set(bytes);
            }
            byteBuf.getBytes(byteBuf.readerIndex(), bytes, 0, size);

            return new String(bytes, 0, size, charset);
        }
    }

    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
