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
 * A schema for 'Byte'.
 */
public class ByteSchema extends AbstractSchema<Byte> {

    private static final ByteSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = new SchemaInfoImpl()
            .setName("INT8")
            .setType(SchemaType.INT8)
            .setSchema(new byte[0]);
        INSTANCE = new ByteSchema();
    }

    public static ByteSchema of() {
        return INSTANCE;
    }

    @Override
    public void validate(byte[] message) {
        if (message.length != 1) {
            throw new SchemaSerializationException("Size of data received by ByteSchema is not 1");
        }
    }

    @Override
    public void validate(ByteBuf message) {
        if (message.readableBytes() != 1) {
            throw new SchemaSerializationException("Size of data received by ByteSchema is not 1");
        }
    }

    @Override
    public byte[] encode(Byte message) {
        if (null == message) {
            return null;
        } else {
            return new byte[]{message};
        }
    }

    @Override
    public Byte decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        validate(bytes);
        return bytes[0];
    }

    @Override
    public Byte decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        }
        validate(byteBuf);
        return byteBuf.getByte(0);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
