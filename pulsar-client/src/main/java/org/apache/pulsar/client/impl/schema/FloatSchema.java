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
 * A schema for `Float`.
 */
public class FloatSchema extends AbstractSchema<Float> {

    private static final FloatSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = new SchemaInfoImpl()
                .setName("Float")
                .setType(SchemaType.FLOAT)
                .setSchema(new byte[0]);
        INSTANCE = new FloatSchema();
    }

    public static FloatSchema of() {
        return INSTANCE;
    }

    @Override
    public void validate(byte[] message) {
        if (message.length != 4) {
            throw new SchemaSerializationException("Size of data received by FloatSchema is not 4");
        }
    }

    @Override
    public void validate(ByteBuf message) {
        if (message.readableBytes() != 4) {
            throw new SchemaSerializationException("Size of data received by FloatSchema is not 4");
        }
    }

    @Override
    public byte[] encode(Float message) {
        if (null == message) {
            return null;
        } else {
            long bits = Float.floatToRawIntBits(message);
            return new byte[] {
                (byte) (bits >>> 24),
                (byte) (bits >>> 16),
                (byte) (bits >>> 8),
                (byte) bits
            };
        }
    }

    @Override
    public Float decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        validate(bytes);
        int value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return Float.intBitsToFloat(value);
    }

    @Override
    public Float decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        }
        validate(byteBuf);
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            value |= byteBuf.getByte(i) & 0xFF;
        }

        return Float.intBitsToFloat(value);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
