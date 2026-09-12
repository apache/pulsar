/*
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
package org.apache.pulsar.common.schema;

import java.nio.ByteBuffer;

/**
 * SchemaIdUtil to fill and retrieve schema id in MessageMetadata.
 */
public class SchemaIdUtil {

    public static final byte MAGIC_BYTE_VALUE = -1;
    public static final byte MAGIC_BYTE_KEY_VALUE = -2;

    public static byte[] addMagicHeader(byte[] data, boolean isKeyValue) {
        if (data == null || data.length == 0) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(1 + data.length);
        buffer.put(isKeyValue ? MAGIC_BYTE_KEY_VALUE : MAGIC_BYTE_VALUE);
        buffer.put(data);
        return buffer.array();
    }

    public static byte[] removeMagicHeader(byte[] schemaId) {
        if (schemaId == null || schemaId.length == 0) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(schemaId);
        byte magicByte = buffer.get();
        if (magicByte != MAGIC_BYTE_VALUE && magicByte != MAGIC_BYTE_KEY_VALUE) {
            return schemaId;
        }
        byte[] id = new byte[buffer.remaining()];
        buffer.get(id);
        return id;
    }

}
