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
package org.apache.pulsar.schema;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.pulsar.client.api.EncodeData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class MockExternalJsonSchema<T> implements Schema<T> {

    public static final byte[] MOCK_SCHEMA_DATA = new byte[] {1, 2, 3, 4, 5};
    public static final byte[] MOCK_KEY_SCHEMA_DATA = new byte[] {0, 4, 3, 4, 8};
    public static final byte[] MOCK_SCHEMA_ID = new byte[] {2, 3, 7, 8};
    public static final byte[] MOCK_KEY_SCHEMA_ID = new byte[] {5, 6, 7, 8, 0, 0, 2};

    private final Class<T> clazz;
    private final boolean isKey;
    @Getter
    private boolean isClosed;

    public MockExternalJsonSchema(Class<T> clazz, boolean isKey) {
        this.clazz = clazz;
        this.isKey = isKey;
    }

    public MockExternalJsonSchema(Class<T> clazz) {
        this.clazz = clazz;
        this.isKey = false;
    }

    @Override
    public EncodeData encode(String topic, T message) {
        // the external schema should register schema when encoding the message, this is just a mock implementation
        return new EncodeData(
                isKey ? MOCK_KEY_SCHEMA_DATA : MOCK_SCHEMA_DATA,
                isKey ? MOCK_KEY_SCHEMA_ID : MOCK_SCHEMA_ID);
    }

    @Override
    public T decode(String topic, ByteBuffer data, byte[] schemaId) {
        return decode(topic, ByteBufUtil.getBytes(Unpooled.wrappedBuffer(data)), schemaId);
    }

    @Override
    public T decode(String topic, byte[] data, byte[] schemaId) {
        byte[] expectedSchemaId = isKey ? MOCK_KEY_SCHEMA_ID : MOCK_SCHEMA_ID;
        if (!Arrays.equals(schemaId, expectedSchemaId)) {
            throw new IllegalStateException("Unexpected schema id");
        }
        byte[] expectedSchemaData = isKey ? MOCK_KEY_SCHEMA_DATA : MOCK_SCHEMA_DATA;
        if (!Arrays.equals(data, expectedSchemaData)) {
            throw new IllegalStateException("Unexpected schema data");
        }
        // the external schema should retrieve the schema and decoding the payload, this is just a mock implementation
        return null;
    }

    @Override
    public byte[] encode(T message) {
        // the external schema doesn't support this method
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SchemaInfoImpl.builder()
                .name("")
                .type(SchemaType.EXTERNAL)
                .schema(new byte[0])
                .build();
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        this.isClosed = true;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Schema<T> clone() {
        return new MockExternalJsonSchema<T>(clazz, isKey);
    }

}
