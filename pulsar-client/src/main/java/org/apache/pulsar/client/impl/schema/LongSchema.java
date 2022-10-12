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
 * A schema for `Long`.
 */
public class LongSchema extends AbstractSchema<Long> {

    private static final LongSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = SchemaInfoImpl.builder()
            .name("INT64")
            .type(SchemaType.INT64)
            .schema(new byte[0]).build();
        INSTANCE = new LongSchema();
    }

    public static LongSchema of() {
        return INSTANCE;
    }

    @Override
    public void validate(byte[] message) {
        if (message.length != 8) {
            throw new SchemaSerializationException("Size of data received by LongSchema is not 8");
        }
    }

    @Override
    public void validate(ByteBuf message) {
        if (message.readableBytes() != 8) {
            throw new SchemaSerializationException("Size of data received by LongSchema is not 8");
        }
    }

    @Override
    public byte[] encode(Long data) {
        if (null == data) {
            return null;
        } else {
            return new byte[] {
                (byte) (data >>> 56),
                (byte) (data >>> 48),
                (byte) (data >>> 40),
                (byte) (data >>> 32),
                (byte) (data >>> 24),
                (byte) (data >>> 16),
                (byte) (data >>> 8),
                data.byteValue()
            };
        }
    }

    @Override
    public Long decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        validate(bytes);
        long value = 0L;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    @Override
    public Long decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        }
        validate(byteBuf);
        long value = 0L;
        for (int i = 0; i < 8; i++) {
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
