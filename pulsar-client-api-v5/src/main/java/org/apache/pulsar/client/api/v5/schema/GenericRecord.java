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
package org.apache.pulsar.client.api.v5.schema;

import java.util.List;

/**
 * A schema-less view over a decoded message value, used when the value's schema is discovered at
 * runtime rather than known at compile time (see {@link Schema#autoConsume()}).
 *
 * <p>The shape of the value depends on {@link #schemaType()}:
 * <ul>
 *   <li>{@link SchemaType#AVRO}, {@link SchemaType#JSON}, {@link SchemaType#PROTOBUF_NATIVE}: a
 *       structured record — use {@link #fields()} and {@link #field(String)} to read its fields. A
 *       field value may itself be a {@link GenericRecord} (nested record).</li>
 *   <li>{@link SchemaType#KEY_VALUE}: {@link #fields()} is empty and {@link #nativeObject()} is a
 *       {@link KeyValue}.</li>
 *   <li>any primitive type (e.g. {@link SchemaType#STRING}, {@link SchemaType#INT32}):
 *       {@link #fields()} is empty and {@link #nativeObject()} is the primitive value.</li>
 * </ul>
 */
public interface GenericRecord {

    /**
     * The schema type of the decoded value.
     *
     * @return the {@link SchemaType}
     */
    SchemaType schemaType();

    /**
     * The underlying decoded value: a {@link KeyValue} for {@link SchemaType#KEY_VALUE}, a primitive
     * for a primitive schema, or an implementation-specific native record for structured types.
     *
     * @return the native object, or {@code null}
     */
    Object nativeObject();

    /**
     * The fields of this record, in declaration order. Empty for non-record (primitive or
     * key/value) values.
     *
     * @return the list of {@link Field}s
     */
    List<Field> fields();

    /**
     * Get the value of a field by name.
     *
     * @param fieldName the field name
     * @return the field value, or {@code null} if absent
     */
    Object field(String fieldName);

    /**
     * Get the value of a field.
     *
     * @param field the field
     * @return the field value, or {@code null} if absent
     */
    default Object field(Field field) {
        return field(field.name());
    }
}
