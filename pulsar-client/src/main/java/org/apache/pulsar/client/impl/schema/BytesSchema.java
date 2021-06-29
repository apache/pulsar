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
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema for bytes array.
 */
public class BytesSchema extends AbstractSchema<byte[]> {

    private static final BytesSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = new SchemaInfoImpl()
            .setName("Bytes")
            .setType(SchemaType.BYTES)
            .setSchema(new byte[0]);
        INSTANCE = new BytesSchema();
    }

    public static BytesSchema of() {
        return INSTANCE;
    }

    @Override
    public byte[] encode(byte[] message) {
        return message;
    }

    @Override
    public byte[] decode(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] decode(ByteBuf byteBuf) {
        if (byteBuf == null) {
            return null;
        }
        int size = byteBuf.readableBytes();
        byte[] bytes = new byte[size];

        byteBuf.getBytes(byteBuf.readerIndex(), bytes);
        return bytes;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
