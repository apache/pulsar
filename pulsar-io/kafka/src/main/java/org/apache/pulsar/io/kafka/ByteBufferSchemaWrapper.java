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
package org.apache.pulsar.io.kafka;

import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * This is a ByteBuffer schema that reports SchemaInfo from another Schema instance.
 */
class ByteBufferSchemaWrapper implements Schema<ByteBuffer> {
    private final Supplier<SchemaInfo> original;

    public ByteBufferSchemaWrapper(Schema original) {
        this(original::getSchemaInfo);
    }

    public ByteBufferSchemaWrapper(SchemaInfo info) {
        this(() -> info);
    }

    public ByteBufferSchemaWrapper(Supplier<SchemaInfo> original) {
        this.original = original;
    }

    @Override
    public byte[] encode(ByteBuffer message) {
        return getBytes(message);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return original.get();
    }

    @Override
    public Schema<ByteBuffer> clone() {
        return new ByteBufferSchemaWrapper(original);
    }

    @Override
    public ByteBuffer decode(byte[] bytes, byte[] schemaVersion) {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return original.get().toString();
    }

    static byte[] getBytes(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (buffer.hasArray() && buffer.arrayOffset() == 0) {
            // do not copy data if the ByteBuffer is a simple wrapper over an array
            byte[] array = buffer.array();
            if (array.length == remaining) {
                return array;
            }
        }
        buffer.mark();
        byte[] avroEncodedData = new byte[remaining];
        buffer.get(avroEncodedData);
        buffer.reset();
        return avroEncodedData;
    }
}
