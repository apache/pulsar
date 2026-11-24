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

import java.util.List;
import org.apache.pulsar.client.api.v5.schema.Field;
import org.apache.pulsar.client.api.v5.schema.GenericRecord;
import org.apache.pulsar.client.api.v5.schema.KeyValue;
import org.apache.pulsar.client.api.v5.schema.SchemaType;

/**
 * Adapts a v4 {@link org.apache.pulsar.client.api.schema.GenericRecord} (produced by the v4
 * {@code AUTO_CONSUME} schema) to the v5 {@link GenericRecord} interface. Nested records and
 * {@code KEY_VALUE} native objects are converted lazily on access so the v5 API never leaks v4
 * types.
 */
final class GenericRecordV5 implements GenericRecord {

    private final org.apache.pulsar.client.api.schema.GenericRecord v4Record;
    private final List<Field> fields;

    private GenericRecordV5(org.apache.pulsar.client.api.schema.GenericRecord v4Record) {
        this.v4Record = v4Record;
        this.fields = v4Record.getFields().stream()
                .map(f -> new Field(f.getName(), f.getIndex()))
                .toList();
    }

    /**
     * Convert a decoded value into its v5-facing form: a v4 {@code GenericRecord} becomes a
     * {@link GenericRecordV5}, a v4 {@code KeyValue} becomes a v5 {@link KeyValue} (with both sides
     * converted recursively), and anything else (primitive, byte[], ...) is returned unchanged.
     */
    static Object convert(Object value) {
        if (value instanceof org.apache.pulsar.client.api.schema.GenericRecord gr) {
            return new GenericRecordV5(gr);
        }
        if (value instanceof org.apache.pulsar.common.schema.KeyValue<?, ?> kv) {
            return new KeyValue<>(convert(kv.getKey()), convert(kv.getValue()));
        }
        return value;
    }

    @Override
    public SchemaType schemaType() {
        org.apache.pulsar.common.schema.SchemaType v4Type = v4Record.getSchemaType();
        if (v4Type == null) {
            return SchemaType.NONE;
        }
        try {
            return SchemaType.valueOf(v4Type.name());
        } catch (IllegalArgumentException e) {
            // v4 has primitive types with no v5 counterpart (e.g. date/time variants); they are
            // handled by the generic "primitive" path, so NONE is a safe mapping.
            return SchemaType.NONE;
        }
    }

    @Override
    public Object nativeObject() {
        return convert(v4Record.getNativeObject());
    }

    @Override
    public List<Field> fields() {
        return fields;
    }

    @Override
    public Object field(String fieldName) {
        return convert(v4Record.getField(fieldName));
    }
}
