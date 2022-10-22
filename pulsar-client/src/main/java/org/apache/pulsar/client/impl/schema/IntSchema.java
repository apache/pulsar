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
 * A schema for `Integer`.
 */
public class IntSchema extends AbstractSchema<Integer> {

    private static final IntSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = SchemaInfoImpl.builder()
            .name("INT32")
            .type(SchemaType.INT32)
            .schema(new byte[0]).build();
        INSTANCE = new IntSchema();
    }

    public static IntSchema of() {
        return INSTANCE;
    }

    @Override
    public void validate(byte[] message) {
        if (message.length != 4) {
            throw new SchemaSerializationException("Size of data received by IntSchema is not 4");
        }
    }

    @Override
    public void validate(ByteBuf message) {
        if (message.readableBytes() != 4) {
            throw new SchemaSerializationException("Size of data received by IntSchema is not 4");
        }
    }

    @Override
    public byte[] encode(Integer message) {
        if (null == message) {
            return null;
        } else {
            return new byte[] {
                (byte) (message >>> 24),
                (byte) (message >>> 16),
                (byte) (message >>> 8),
                message.byteValue()
            };
        }
    }

    @Override
    public Integer decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        validate(bytes);
        int value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    @Override
    public Integer decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        }
        validate(byteBuf);
        int value = 0;

        for (int i = 0; i < 4; i++) {
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
