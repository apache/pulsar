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
package org.apache.pulsar.client.impl.v5;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.api.v5.schema.SchemaInfo;
import org.apache.pulsar.client.api.v5.schema.SchemaType;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;

/**
 * Adapts between v5 Schema and v4 Schema interfaces.
 */
final class SchemaAdapter {

    private SchemaAdapter() {
    }

    /**
     * Wrap a v4 Schema as a v5 Schema.
     */
    static <T> Schema<T> toV5(org.apache.pulsar.client.api.Schema<T> v4Schema) {
        return new V5SchemaWrapper<>(v4Schema);
    }

    /**
     * Unwrap a v5 Schema to get the underlying v4 Schema.
     * If it's already a wrapper, extract the inner v4 schema.
     * Otherwise, create a v4 adapter around the v5 schema.
     */
    @SuppressWarnings("unchecked")
    static <T> org.apache.pulsar.client.api.Schema<T> toV4(Schema<T> v5Schema) {
        if (v5Schema instanceof V5SchemaWrapper<T> wrapper) {
            return wrapper.v4Schema;
        }
        return new V4SchemaWrapper<>(v5Schema);
    }

    /**
     * Wraps a v4 Schema as a v5 Schema.
     */
    private static final class V5SchemaWrapper<T> implements Schema<T> {
        final org.apache.pulsar.client.api.Schema<T> v4Schema;

        V5SchemaWrapper(org.apache.pulsar.client.api.Schema<T> v4Schema) {
            this.v4Schema = v4Schema;
        }

        @Override
        public byte[] encode(T message) {
            return v4Schema.encode(message);
        }

        @Override
        public T decode(byte[] bytes) {
            return v4Schema.decode(bytes);
        }

        @Override
        public T decode(byte[] bytes, byte[] schemaVersion) {
            return v4Schema.decode(bytes, schemaVersion);
        }

        @Override
        public T decode(ByteBuffer data) {
            return v4Schema.decode(data);
        }

        @Override
        public SchemaInfo schemaInfo() {
            var v4Info = v4Schema.getSchemaInfo();
            if (v4Info == null) {
                return null;
            }
            return new SchemaInfoV5(v4Info);
        }
    }

    /**
     * Wraps a v5 Schema as a v4 Schema (for passing to ProducerImpl/ConsumerImpl).
     */
    private static final class V4SchemaWrapper<T> implements org.apache.pulsar.client.api.Schema<T> {
        final Schema<T> v5Schema;

        V4SchemaWrapper(Schema<T> v5Schema) {
            this.v5Schema = v5Schema;
        }

        @Override
        public byte[] encode(T message) {
            return v5Schema.encode(message);
        }

        @Override
        public T decode(byte[] bytes) {
            return v5Schema.decode(bytes);
        }

        @Override
        public T decode(byte[] bytes, byte[] schemaVersion) {
            return v5Schema.decode(bytes, schemaVersion);
        }

        @Override
        public org.apache.pulsar.common.schema.SchemaInfo getSchemaInfo() {
            var v5Info = v5Schema.schemaInfo();
            if (v5Info == null) {
                return null;
            }
            return SchemaInfoImpl.builder()
                    .name(v5Info.name())
                    .type(org.apache.pulsar.common.schema.SchemaType.valueOf(v5Info.type().name()))
                    .schema(v5Info.schema())
                    .properties(v5Info.properties())
                    .build();
        }

        @Override
        public org.apache.pulsar.client.api.Schema<T> clone() {
            return this;
        }

        @Override
        public void validate(byte[] message) {
        }

        @Override
        public boolean supportSchemaVersioning() {
            return false;
        }

        @Override
        public Optional<Object> getNativeSchema() {
            return Optional.empty();
        }

        @Override
        public boolean requireFetchingSchemaInfo() {
            return false;
        }

        @Override
        public void configureSchemaInfo(String topic, String componentName,
                                        org.apache.pulsar.common.schema.SchemaInfo schemaInfo) {
        }
    }

    /**
     * Adapts a v4 SchemaInfo to the v5 SchemaInfo interface.
     */
    private record SchemaInfoV5(
            org.apache.pulsar.common.schema.SchemaInfo v4Info
    ) implements SchemaInfo {

        @Override
        public String name() {
            return v4Info.getName();
        }

        @Override
        public SchemaType type() {
            return SchemaType.valueOf(v4Info.getType().name());
        }

        @Override
        public byte[] schema() {
            return v4Info.getSchema();
        }

        @Override
        public Map<String, String> properties() {
            return v4Info.getProperties();
        }
    }
}
