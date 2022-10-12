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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A variant `Bytes` schema that takes {@link io.netty.buffer.ByteBuf}.
 */
public class ByteBufSchema extends AbstractSchema<ByteBuf> {

    private static final ByteBufSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = SchemaInfoImpl.builder()
            .name("ByteBuf")
            .type(SchemaType.BYTES)
            .schema(new byte[0]).build();
        INSTANCE = new ByteBufSchema();
    }

    public static ByteBufSchema of() {
        return INSTANCE;
    }

    @Override
    public byte[] encode(ByteBuf message) {
        if (message == null) {
            return null;
        }

        return ByteBufUtil.getBytes(message);
    }

    @Override
    public ByteBuf decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        } else {
            return Unpooled.wrappedBuffer(bytes);
        }
    }

    @Override
    public ByteBuf decode(ByteBuf byteBuf) {
        return byteBuf;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
