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
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema for `Short`.
 */
public class ShortSchema extends AbstractSchema<Short> {

    private static final ShortSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = new SchemaInfoImpl()
            .setName("INT16")
            .setType(SchemaType.INT16)
            .setSchema(new byte[0]);
        INSTANCE = new ShortSchema();
    }

    public static ShortSchema of() {
        return INSTANCE;
    }

    @Override
    public void validate(byte[] message) {
        if (message.length != 2) {
            throw new SchemaSerializationException("Size of data received by ShortSchema is not 2");
        }
    }

    @Override
    public void validate(ByteBuf message) {
        if (message.readableBytes() != 2) {
            throw new SchemaSerializationException("Size of data received by ShortSchema is not 2");
        }
    }

    @Override
    public byte[] encode(Short message) {
        if (null == message) {
            return null;
        } else {
            return new byte[] {
                (byte) (message >>> 8),
                message.byteValue()
            };
        }
    }

    @Override
    public Short decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        validate(bytes);
        short value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    @Override
    public Short decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        }
        validate(byteBuf);
        short value = 0;

        for (int i = 0; i < 2; i++) {
            value <<= 8;
            value |= byteBuf.getByte(i) & 0xFF;
        }
        return value;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
